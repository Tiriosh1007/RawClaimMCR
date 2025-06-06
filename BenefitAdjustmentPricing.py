import pandas as pd
import numpy as np
import os
import datetime as dt
import warnings
warnings.filterwarnings('ignore')

class BenefitAdjustmentPricing():
    def __init__(self):
        # self.ip_incurred_df = pd.DataFrame()
        # self.ip_paid_df_original = pd.DataFrame()
        # self.original_op_df = pd.DataFrame()
        self.claimant_info_col = ['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age']
        self.op_visit_cols = ['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)', 'Diagnostic: X-Ray & Lab Test (DX)', 'Prescribed Medicine (PM)']
        self.total_visit_cols = ['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)']
        self.op_paid_per_claim_cols = [f'{col}_paid_per_claim' for col in self.op_visit_cols]
        self.total_paid_per_claim_cols = [f'{col}_paid_per_claim' for col in self.total_visit_cols]

    def read_data(self, freq_analysis_fp):
        self.ip_incurred_df = pd.read_excel(freq_analysis_fp, 
                                            sheet_name='Claimant IP Usage Incurred',
                                            )
        self.ip_incurred_df.set_index(self.claimant_info_col, inplace=True)
        self.ip_paid_df_original = pd.read_excel(freq_analysis_fp,
                                                 sheet_name='Claimant IP Usage',
                                                 )
        self.ip_paid_df_original.set_index(self.claimant_info_col, inplace=True)
        self.original_op_df = pd.read_excel(freq_analysis_fp,
                                            sheet_name='Claimant Visits',
                                            )
        self.original_op_df.set_index(self.claimant_info_col, inplace=True)
        
    def ip_used_benefits(self):
        self.ip_used_benefits_cols = self.ip_incurred_df.columns.tolist().remove('days_cover')
        return self.ip_incurred_df.columns.tolist().remove('days_cover')
    
    def ip_existing_paid(self):
        return self.ip_paid_df_original[self.ip_used_benefits_cols].sum(axis=1).sum()
    
    def op_existing_paid(self):
        return self.original_op_df['total_paid'].sum() + self.original_op_df['DX_paid'].sum() + self.original_op_df['PM_paid'].sum()
    
    def op_adjustment(self, class_l: list, benefits_l: list, visits_l:list, amount_per_visit_l: list):
        """
        : 1. Adjust the itemised benefits
        : 2. Adjust sublimit benefits visits (only care the visits)
        : 3. Adjust the total number of visits
        : 4. Adjust DX_paid and PM_paid
        : 5. Calculate Total
        : Note!!! The benefits_l has to be itemise first, then sublimit, then total.
        """
        self.op_adjustment_reduction = []
        self.op_adjustment_df = self.original_op_df.copy().fillna(0)
        self.op_adjustment_df.reset_index(inplace=True)
        for class_, benefit_, visits_, amount_per_visit_ in zip(class_l, benefits_l, visits_l, amount_per_visit_l):
            if 'Total' not in benefit_ and '+' not in benefit_ and 'DX' not in benefit_ and 'PM' not in benefit_:
                if visits_ != None:
                    if class_ != 'all':
                        self.op_adjustment_df[benefit_].loc[(self.op_adjustment_df['class'] == class_) & (self.op_adjustment_df[benefit_] > visits_)] = visits_
                    else:
                        self.op_adjustment_df[benefit_].loc[self.op_adjustment_df[benefit_] > visits_] = visits_
                if amount_per_visit_ != None:
                    if class_ != 'all':
                        self.op_adjustment_df[f'{benefit_}_paid_per_claim'].loc[(self.op_adjustment_df['class'] == class_) & (self.op_adjustment_df[f'{benefit_}_paid_per_claim'] > amount_per_visit_)] = amount_per_visit_
                    else:
                        self.op_adjustment_df[f'{benefit_}_paid_per_claim'].loc[(self.op_adjustment_df[f'{benefit_}_paid_per_claim'] > amount_per_visit_)] = amount_per_visit_
            # Please note that for DX and PM, the amount_per_visit_ is the total amount for the whole claim
            elif 'DX' in benefit_ or 'PM' in benefit_:
                if amount_per_visit_ != None:
                    if class_ != 'all':
                        self.op_adjustment_df[f'{benefit_}_paid'].loc[(self.op_adjustment_df['class'] == class_) & (self.op_adjustment_df[f'{benefit_}_paid'] > amount_per_visit_)] = amount_per_visit_
                    else:
                        self.op_adjustment_df[f'{benefit_}_paid'].loc[self.op_adjustment_df[f'{benefit_}_paid'] > amount_per_visit_] = amount_per_visit_
            else:
                if '+' in benefit_:
                    sublimits_ = benefit_.split('+')
                else: 
                    sublimits_ = self.total_visit_cols
                self.op_adjustment_df[benefit_] = self.op_adjustment_df[sublimits_].sum(axis=1)
                self.op_adjustment_df[benefit_].loc[self.op_adjustment_df[benefit_] <= visits_] = 1
                self.op_adjustment_df[benefit_].loc[self.op_adjustment_df[benefit_] > visits_] = visits_ / self.op_adjustment_df[benefit_].loc[self.op_adjustment_df[benefit_] > visits_]
                for s_ in sublimits_:
                    if class_ != 'all':
                        self.op_adjustment_df[s_].loc[(self.op_adjustment_df['class'] == class_)] = self.op_adjustment_df[s_].loc[(self.op_adjustment_df['class'] == class_)] *\
                              self.op_adjustment_df[benefit_].loc[(self.op_adjustment_df['class'] == class_)]
                    else:
                        self.op_adjustment_df[s_] = self.op_adjustment_df[s_] * self.op_adjustment_df[benefit_]
                self.op_adjustment_df.drop(columns=benefit_, inplace=True)

        for c in (self.total_visit_cols + self.total_paid_per_claim_cols):
            self.op_adjustment_df[c] = self.op_adjustment_df[c].fillna(0)
        self.op_adjustment_df['total_claims'] = self.op_adjustment_df[self.total_visit_cols].sum(axis=1)
        self.op_adjustment_df['total_paid'] = 0
        for b_, p_ in zip(self.total_visit_cols, self.total_paid_per_claim_cols):
            self.op_adjustment_df['total_paid'] = self.op_adjustment_df['total_paid'] + (self.op_adjustment_df[b_] * self.op_adjustment_df[p_])
        
        self.op_adjustment_reduction.append(self.original_op_df['total_paid'].sum() - self.op_adjustment_df['total_paid'].sum())

        return self.op_adjustment_df
    
    def ip_adjustment(self, class_l: list, benefits_l: list, amount_limit_l: list, rmb_l: list, policy_disability:str):
        """
        : 1. Adjust the itemised benefits
        : 2. Calculate Total
        : Note!!! The benefits_l has to be itemise first, then total.
        """
        

        
        self.ip_adjustment_df = self.ip_paid_df_original.copy().fillna(0)
        self.ip_cal_df = self.ip_incurred_df.copy().fillna(0)
        self.ip_adjustment_df.reset_index(inplace=True)
        self.ip_cal_df.reset_index(inplace=True)
        if policy_disability != 'per_dis_per_year':
            self.ip_cal_df = self.ip_cal_df.groupby(by=self.claimant_info_col).sum().reset_index()
            self.ip_adjustment_df = self.ip_adjustment_df.groupby(by=self.claimant_info_col).sum().reset_index()

        days_related = ['Daily Room & Board', "In-hosptial Doctor's Visit", "Companion Bed", "Specialist's Fee (IP)", "Intensive Care Unit",
                        "Special Registered Nursing", "Pre&Post Hosp Cinic Consultation", "Daily Cash Benefit (HA's Ward)", 
                        "Secondary Claim Incentive/ Hospital Income for Coordination"]
        for class_, benefit_, amount_limit_, rmb_ in zip(class_l, benefits_l, amount_limit_l, rmb_):
            if 'SMM' not in benefit_:
                if benefit_ in days_related:
                    if class_ != 'all':
                        self.ip_adjustment_df[benefit_].loc[
                            (amount_limit_ * self.ip_cal_df['days_cover'].loc[(self.ip_cal_df['class'] == class_)] > self.ip_cal_df[benefit_].loc[(self.ip_cal_df['class'] == class_)])]\
                                  = amount_limit_ * self.ip_cal_df['days_cover'].loc[(self.ip_cal_df['class'] == class_)]
                    else:
                        self.ip_adjustment_df[benefit_].loc[
                            (amount_limit_ * self.ip_cal_df['days_cover'] > self.ip_cal_df[benefit_])]\
                                  = amount_limit_ * self.ip_cal_df['days_cover']
                else:
                    if class_ != 'all':
                        self.ip_adjustment_df[benefit_].loc[(self.ip_cal_df[benefit_] * rmb_ > amount_limit_) & (self.ip_cal_df['class'] == class_)] = amount_limit_
                    else:
                        self.ip_adjustment_df[benefit_].loc[(self.ip_cal_df[benefit_] * rmb_ > amount_limit_)] = amount_limit_


    