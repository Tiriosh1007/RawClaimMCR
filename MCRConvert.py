import pandas as pd
from openpyxl import load_workbook

class MCRConvert():
    def __init__(self, input_file):
        try:
            self.input_file = input_file
            
            # template_file = "GMI_template.xlsx"
            self.template_file = "GMI_template.xlsx"
            
            self.template_wb = load_workbook(self.template_file, keep_links=False, data_only=False)

            self.input_dtype = {
                "policy_number": str,
                "year": str,
                "benefit_type": str,
                "benefit": str,
                "panel": str,
                "class": str,
                "incurred_amount": float,
                "paid_amount": float,
                "usage_ratio": float,
                # "no_of_claims": int,
                # "no_of_claimants": int,
                "incurred_per_claim": float,
                "paid_per_claim": float,
                "incurred_per_claimant": float,
                "paid_per_claimant": float,
                "claim_frequency": float,
            }

            self.mcr_p20_policy = pd.read_excel(self.input_file, sheet_name='P.20_Policy', dtype=self.input_dtype)
            self.mcr_p20_benefit = pd.read_excel(self.input_file, sheet_name='P.20_BenefitType', dtype=self.input_dtype, na_values=0)
            self.mcr_p20_network = pd.read_excel(self.input_file, sheet_name='P.20_Network', dtype=self.input_dtype, na_values=0)
            self.mcr_p21_class = pd.read_excel(self.input_file, sheet_name='P.21_Class', dtype=self.input_dtype)
            self.mcr_p22_class_benefit = pd.read_excel(self.input_file, sheet_name='P.22_Class_BenefitType', dtype=self.input_dtype, na_values=0)
            self.mcr_p25_class_panel_benefit = pd.read_excel(self.input_file, sheet_name='P.25_Class_Panel_BenefitType', dtype=self.input_dtype, na_values=0)
            self.mcr_p26_op_panel_benefit = pd.read_excel(self.input_file, sheet_name='P.26_OP_Panel_Benefit', dtype=self.input_dtype, na_values=0)

            self.policy_info = self.mcr_p20_policy[["policy_number", "year"]]

        except FileNotFoundError:
            print(f"Error: The file '{self.input_file}' was not found. Please check the name and try again.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def set_policy_input(self,
                         previous_policy_num, previous_year_start_date, previous_year_end_date, previous_year, 
                         current_policy_num, current_year_start_date, current_year_end_date, current_year
                         ):
        self.previous_year, self.current_year = previous_year, current_year
        self.previous_year_start_date, self.previous_year_end_date = previous_year_start_date, previous_year_end_date
        self.current_year_start_date, self.current_year_end_date = current_year_start_date, current_year_end_date
        self.previous_policy_num, self.current_policy_num = previous_policy_num, current_policy_num
        self.plan_info = self.mcr_p21_class.copy(deep=True)
        self.plan_info.dropna(inplace=True)
        self.plan_info = self.plan_info.loc[(self.plan_info["policy_number"] == self.current_policy_num) & 
                                            (self.plan_info["year"] == self.current_year), "class"].tolist()
        
    
    def claim_info(self):
        claim_info_worksheet = self.template_wb["ClaimInfo"]

        if self.previous_policy_num is not None:
            claim_info_worksheet.cell(row=4,column=3).value = self.previous_policy_num
            claim_info_worksheet.cell(row=6,column=3).value = self.previous_year_start_date
            claim_info_worksheet.cell(row=7,column=3).value = self.previous_year_end_date

        if self.current_policy_num is not None:
            claim_info_worksheet.cell(row=9,column=3).value = self.current_policy_num
            claim_info_worksheet.cell(row=11,column=3).value = self.current_year_start_date
            claim_info_worksheet.cell(row=12,column=3).value = self.current_year_end_date

        plan_start_row = 2
        for i in range(plan_start_row, len(self.plan_info) + 2):
            claim_info_worksheet.cell(row=i, column=8).value = self.plan_info[i-2]
        
                

    def P20_overall(self):
        input_p20 = self.mcr_p20_policy.loc[((self.mcr_p20_policy["policy_number"] == self.current_policy_num) & (self.mcr_p20_policy["year"] == self.current_year)) | 
                                            ((self.mcr_p20_policy["policy_number"] == self.previous_policy_num) & (self.mcr_p20_policy["year"] == self.previous_year))].dropna()
        template_p20 = self.template_wb["P20_Usage Overview"]

        previous_start_row = current_start_row = 10
        for index, row in input_p20.iterrows():
            if row['year'] == self.previous_year:
                previous_start_row += 1 
                for col, val in zip([2,3,4], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                    template_p20.cell(row=previous_start_row, column=1).value = self.previous_policy_num
                    template_p20.cell(row=previous_start_row, column=col).value = val
            
            elif row['year'] == self.current_year:
                current_start_row += 1
                for col, val in zip([6,7,8], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                    template_p20.cell(row=previous_start_row, column=5).value = self.current_policy_num
                    template_p20.cell(row=current_start_row, column=col).value = val


    def P20_benefittype(self):
        input_p20_btype = self.mcr_p20_benefit.loc[((self.mcr_p20_benefit["policy_number"] == self.current_policy_num) & (self.mcr_p20_benefit["year"] == self.current_year)) | 
                                                   ((self.mcr_p20_benefit["policy_number"] == self.previous_policy_num) & (self.mcr_p20_benefit["year"] == self.previous_year))].dropna()
        template_p20 = self.template_wb["P20_Usage Overview"]

        # by benefit table variables 
        previous_start_row = current_start_row = 15

        # number of claims table variables 
        previous_num_of_claim = current_num_of_claim = 33

        for index, row in input_p20_btype.iterrows():
            if row["benefit_type"] != 'Total':
                if row["year"] == self.previous_year:
                    previous_start_row +=1
                    previous_num_of_claim +=1
                    template_p20.cell(row=previous_num_of_claim, column=2).value = row['no_of_claims']
                    for col, val in zip([2,3,4], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):                      
                        template_p20.cell(row=previous_start_row, column=col).value = val

                elif row["year"] == self.current_year:
                    current_start_row += 1
                    current_num_of_claim += 1
                    template_p20.cell(row=current_num_of_claim, column=6).value = row['no_of_claims']
                    for col, val in zip([6,7,8], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                        template_p20.cell(row=current_start_row, column=col).value = val


    def P20_network(self):
        input_p20_btype = self.mcr_p20_network.loc[((self.mcr_p20_network["policy_number"] == self.current_policy_num) & (self.mcr_p20_network["year"] == self.current_year)) | 
                                                   ((self.mcr_p20_network["policy_number"] == self.previous_policy_num) & (self.mcr_p20_network["year"] == self.previous_year))]
        template_p20 = self.template_wb["P20_Usage Overview"]

        previous_start_row = current_start_row = 27
        for index, row in input_p20_btype.iterrows():
            # Handling the by network table in P20_overview worksheet
            row_offset = 1 if row['panel'] == "Panel" else 0
            if row['year'] == self.previous_year:
                for col, val in zip([2, 3, 4], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                    template_p20.cell(row=previous_start_row + row_offset, column=col).value = val

            elif row['year'] == self.current_year:
                for col, val in zip([6, 7, 8], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):                      
                    template_p20.cell(row=current_start_row + row_offset, column=col).value = val

    
    def P21_by_class(self):
        input_p21 = self.mcr_p21_class.loc[((self.mcr_p21_class["policy_number"] == self.current_policy_num) & (self.mcr_p21_class["year"] == self.current_year)) | 
                                           ((self.mcr_p21_class["policy_number"] == self.previous_policy_num) & (self.mcr_p21_class["year"] == self.previous_year))].dropna()
        template_p21 = self.template_wb["P21_Usage by Class"]

        current_start_row = previous_start_row = 7
        for index, row in input_p21.iterrows():
            if row['year'] == self.current_year:
                current_start_row  += 1 
                # template_p21.cell(row=current_start_row, column=1).value = row['class']
                for col, val in zip([6,7,8], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):                      
                    template_p21.cell(row=current_start_row, column=col).value = val

            elif row['year'] == self.previous_year:
                previous_start_row += 1
                for col, val in zip([3,4,5], [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):                      
                    template_p21.cell(row=previous_start_row, column=col).value = val


    def P22_by_class(self):
        input_p22 = self.mcr_p22_class_benefit.loc[((self.mcr_p22_class_benefit["policy_number"] == self.current_policy_num) & (self.mcr_p22_class_benefit["year"] == self.current_year)) | 
                                                   ((self.mcr_p22_class_benefit["policy_number"] == self.previous_policy_num) & (self.mcr_p22_class_benefit["year"] == self.previous_year))]
        template_p22 = self.template_wb["P22_Usage by Class by Ben"]

        input_p22.fillna(0, inplace=True)

        plan_num = self.plan_info
        P22_format_row_num = [13,20,27,34,45,52,59,66,77,84,91,98]

        plan_num, P22_format_row_num = pd.Series(plan_num).astype(str), pd.Series(P22_format_row_num)
        current_plan_row_df = pd.concat([plan_num, P22_format_row_num],axis=1, ignore_index=True).dropna()
        current_plan_row_df.columns = ['plan', "start_row"]
        
        previous_row_dict = current_plan_row_df.set_index('plan')['start_row'].to_dict()
        current_row_dict = current_plan_row_df.set_index('plan')['start_row'].to_dict()

        for index, row in input_p22.iterrows():
            class_id = row['class']
            previous_start_row = previous_row_dict[class_id]
            current_start_row = current_row_dict[class_id]

            if row['year'] == self.previous_year:
                row_target = previous_start_row + 1
                cols = [3, 4, 5]
                previous_row_dict[class_id] = row_target
                for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                    template_p22.cell(row=row_target, column=col).value = val

            elif row['year'] == self.current_year:
                row_target = current_start_row + 1
                cols = [6, 7, 8]
                current_row_dict[class_id] = row_target
                for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                    template_p22.cell(row=row_target, column=col).value = val
                        
        
    def P25_by_plan(self):
        input_p25 = self.mcr_p25_class_panel_benefit.loc[((self.mcr_p25_class_panel_benefit["policy_number"] == self.current_policy_num) & (self.mcr_p25_class_panel_benefit["year"] == self.current_year)) | 
                                                         ((self.mcr_p25_class_panel_benefit["policy_number"] == self.previous_policy_num) & (self.mcr_p25_class_panel_benefit["year"] == self.previous_year))]
        template_p25 = self.template_wb["P25_UsageByplanBynetworkByben"]
        input_p25.fillna(0, inplace=True)
        plan_num = self.plan_info

        # creating dataframe for handling non-panel data
        P25_nonpanel_row_num = [13,27,41,60,74,88,107,121,135,154,168,182]
        plan_num, P25_nonpanel_row_num = pd.Series(plan_num).astype(str), pd.Series(P25_nonpanel_row_num)
        nonpanel_plan_row_df = pd.concat([plan_num, P25_nonpanel_row_num],axis=1, ignore_index=True).dropna()
        nonpanel_plan_row_df.columns = ['plan', "start_row"]

        previous_nonpanel_row_dict = nonpanel_plan_row_df.set_index('plan')['start_row'].to_dict()
        current_nonpanel_row_dict = nonpanel_plan_row_df.set_index('plan')['start_row'].to_dict()

        # creating dataframe for handling panel data
        P25_panel_row_num  = [20,34,48,67,81,95,114,128,142,161,175,189]
        plan_num, P25_panel_row_num = pd.Series(plan_num).astype(str), pd.Series(P25_panel_row_num)
        panel_plan_row_df = pd.concat([plan_num, P25_panel_row_num],axis=1, ignore_index=True).dropna()
        panel_plan_row_df.columns = ['plan', "start_row"]
        
        previous_panel_row_dict = panel_plan_row_df.set_index('plan')['start_row'].to_dict()
        current_panel_row_dict = panel_plan_row_df.set_index('plan')['start_row'].to_dict()
    
        for index, row in input_p25.iterrows():
            class_id = row['class']
            previous_start_row = previous_nonpanel_row_dict[class_id]
            current_start_row = current_nonpanel_row_dict[class_id]

            previous_panel_start_row = previous_panel_row_dict[class_id]
            current_panel_start_row = current_panel_row_dict[class_id]

            if row['panel'] == 'Non-Panel':
                if row['year'] == self.previous_year:
                    row_target = previous_start_row + 1
                    cols = [4, 5, 6]
                    previous_nonpanel_row_dict[class_id] = row_target
                    for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                        template_p25.cell(row=row_target, column=col).value = val
                elif row['year'] == self.current_year:
                    row_target = current_start_row + 1
                    cols = [7,8,9]
                    current_nonpanel_row_dict[class_id] = row_target
                    for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                        template_p25.cell(row=row_target, column=col).value = val
            
            elif row['panel'] == 'Panel':
                if row['year'] == self.previous_year:
                    row_target = previous_panel_start_row + 1
                    cols = [4, 5, 6]
                    previous_panel_row_dict[class_id] = row_target
                    for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                        template_p25.cell(row=row_target, column=col).value = val
                elif row['year'] == self.current_year:
                    row_target = current_panel_start_row + 1
                    cols = [7,8,9]
                    current_panel_row_dict[class_id] = row_target
                    for col, val in zip(cols, [row['incurred_amount'], row['paid_amount'], row['usage_ratio']]):
                        template_p25.cell(row=row_target, column=col).value = val


    def P26_by_class(self):
        input_p26 = self.mcr_p26_op_panel_benefit.loc[((self.mcr_p26_op_panel_benefit["policy_number"] == self.current_policy_num) & (self.mcr_p26_op_panel_benefit["year"] == self.current_year)) | 
                                                      ((self.mcr_p26_op_panel_benefit["policy_number"] == self.previous_policy_num) & (self.mcr_p26_op_panel_benefit["year"] == self.previous_year))].dropna()
        template_p26 = self.template_wb["P26_Usage_Clinical by Network"]

        panel_previous_start_row = panel_current_start_row = 7
        nonpanel_previous_start_row = nonpanel_current_start_row = 28

        rows = {
            ('Panel', self.current_year): {'row': panel_previous_start_row, 'cols': [2, 9, 10, 11, 12, 13, 14], 'values': ['benefit', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim']},
            ('Panel', self.previous_year): {'row': panel_current_start_row, 'cols': [3, 4, 5, 6, 7, 8], 'values': ['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim']},
            ('Non-Panel', self.current_year): {'row': nonpanel_previous_start_row, 'cols': [2, 9, 10, 11, 12, 13, 14], 'values': ['benefit', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim']},
            ('Non-Panel', self.previous_year): {'row': nonpanel_current_start_row, 'cols': [3, 4, 5, 6, 7, 8], 'values': ['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim']}
        }

        current_rows = {
            ('Panel', self.current_year): panel_previous_start_row,
            ('Panel', self.previous_year): panel_current_start_row,
            ('Non-Panel', self.current_year): nonpanel_previous_start_row,
            ('Non-Panel', self.previous_year): nonpanel_current_start_row
        }

        for index, row in input_p26.iterrows():
            key = (row['panel'], row['year'])
            if key in rows:
                current_rows[key] += 1
                row_idx = current_rows[key]
                info = rows[key]
                values = [row[val] for val in info['values']]
                for col, val in zip(info['cols'], values):
                    template_p26.cell(row=row_idx, column=col).value = val

    def save(self):
        from io import BytesIO
        output_file = BytesIO()
        try:
            # self.template_wb.save(output_file)
            # print(f"File saved successfully as '{output_file}'")
            self.template_wb.save(output_file)
            return output_file.getvalue()
        except Exception as e:
            print(f"An error occurred while saving the file into the BytesIO: {e}")


    def loss_ratio_text_convert(self, previous_yr_loss_ratio_text=None, current_yr_loss_ratio_text = None, loss_ratio_grouping_optical=True):
        self.previous_year_loss_ratio_df, self.current_year_loss_ratio_df = None, None
        print("previous_yr_loss_ratio_text:", previous_yr_loss_ratio_text)
        print("current_yr_loss_ratio_text:", current_yr_loss_ratio_text)
        if previous_yr_loss_ratio_text:
            previous_yr_loss_ratio_text = previous_yr_loss_ratio_text.splitlines()
            data_l = [previous_yr_loss_ratio_text[i].split(",") for i in range(len(previous_yr_loss_ratio_text))]
            self.previous_year_loss_ratio_df = pd.DataFrame(data_l[1:], columns=data_l[0])
            if ("Optical" in self.previous_year_loss_ratio_df.benefit_type.tolist()) & (loss_ratio_grouping_optical == True):
                self.previous_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.previous_year_loss_ratio_df.benefit_type == "Clinical"] = self.previous_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.previous_year_loss_ratio_df.benefit_type == "Clinical"].astype(float) + self.previous_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.previous_year_loss_ratio_df.benefit_type == "Optical"].astype(float)
                self.previous_year_loss_ratio_df.drop("Optical", inplace=True)
            # print(self.previous_year_loss_ratio_df)
        if current_yr_loss_ratio_text:
            current_yr_loss_ratio_text = current_yr_loss_ratio_text.splitlines()
            data_l = [current_yr_loss_ratio_text[i].split(",") for i in range(len(current_yr_loss_ratio_text))]
            self.current_year_loss_ratio_df = pd.DataFrame(data_l[1:], columns=data_l[0])
            if ("Optical" in self.current_year_loss_ratio_df.benefit_type.tolist()) & (loss_ratio_grouping_optical == True):
                self.current_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.current_year_loss_ratio_df.benefit_type == "Clinical"] = self.current_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.current_year_loss_ratio_df.benefit_type == "Clinical"].astype(float) + self.current_year_loss_ratio_df[["actual_premium", "actual_paid_w_ibnr"]].loc[self.current_year_loss_ratio_df.benefit_type == "Optical"].astype(float)
                self.current_year_loss_ratio_df.drop("Optical", inplace=True)
                
            # print(self.current_year_loss_ratio_df)


    def p16_LR_by_benefits(self):
        # self.loss_ratio_text_convert()
        template_p16 = self.template_wb["P16_LR by Benefits"]
        cols = [3,4,5]
        previous_start_row, current_start_row = 5,12
        print(self.previous_year_loss_ratio_df)
        print(self.current_year_loss_ratio_df)
        
        
        if self.previous_year_loss_ratio_df is not None:
            for i in range(previous_start_row, len(self.previous_year_loss_ratio_df) + previous_start_row):
                if self.previous_year_loss_ratio_df.iloc[i-previous_start_row]['benefit_type'] != "Total":
                    template_p16.cell(row=i, column=3).value = float(self.previous_year_loss_ratio_df.iloc[i-previous_start_row]['actual_premium']) * 12 / int(self.previous_year_loss_ratio_df.iloc[i-previous_start_row]["duration"])
                    template_p16.cell(row=i, column=4).value = float(self.previous_year_loss_ratio_df.iloc[i-previous_start_row]['actual_paid_w_ibnr']) * 12 / int(self.previous_year_loss_ratio_df.iloc[i-previous_start_row]["duration"])
                    # template_p16.cell(row=i, column=5).value = float(self.previous_year_loss_ratio_df.iloc[i-previous_start_row]['loss_ratio'])
            # for index, row in self.previous_year_loss_ratio_df.iterrows():
            #     if row['benefit_type'] != "Total":
            #         previous_start_row +=1
            #         row['actual_premium'], row["actual_paid_w_ibnr"] = float(row['actual_premium']) * 12 / int(row["duration"]) , float(row["actual_paid_w_ibnr"]) * 12 / int(row["duration"])
            #         for col, val in zip(cols, [row['actual_premium'], row['actual_paid_w_ibnr'], row['loss_ratio']]):
            #             template_p16.cell(row=previous_start_row, column=col).value = val

        if self.current_year_loss_ratio_df is not None:
            for i in range(current_start_row, len(self.current_year_loss_ratio_df) + current_start_row):
                if self.current_year_loss_ratio_df.iloc[i-current_start_row]['benefit_type'] != "Total":
                    template_p16.cell(row=i, column=3).value = float(self.current_year_loss_ratio_df.iloc[i-current_start_row]['actual_premium']) * 12 / int(self.current_year_loss_ratio_df.iloc[i-current_start_row]["duration"])
                    template_p16.cell(row=i, column=4).value = float(self.current_year_loss_ratio_df.iloc[i-current_start_row]['actual_paid_w_ibnr']) * 12 / int(self.current_year_loss_ratio_df.iloc[i-current_start_row]["duration"])
                    # template_p16.cell(row=i, column=5).value = self.current_year_loss_ratio_df.iloc[i-current_start_row]['loss_ratio']
            # for index, row in self.current_year_loss_ratio_df.iterrows():
            #     if row['benefit_type'] != "Total":
            #         current_start_row +=1
            #         row['actual_premium'], row["actual_paid_w_ibnr"] = float(row['actual_premium']) * 12 / int(row["duration"]) , float(row["actual_paid_w_ibnr"]) * 12 / int(row["duration"])
            #         for col, val in zip(cols, [row['actual_premium'], row['actual_paid_w_ibnr'], row['loss_ratio']]):
            #             template_p16.cell(row=current_start_row, column=col).value = val


    def convert_all(self):
        self.claim_info()
        self.p16_LR_by_benefits()
        self.P20_overall()
        self.P20_benefittype()
        self.P20_network()
        self.P21_by_class()
        self.P22_by_class()
        self.P25_by_plan()
        self.P26_by_class()
    
# def main():
#     GMI = MCRConvert(input_file='input_example_01.xlsx', previous_policy_num="015989", previous_year= 2023, current_policy_num="015989", current_year=2024)
#     GMI.P20_overall()
#     GMI.P20_network()
#     GMI.P20_benefittype()
#     GMI.P21_by_class()
#     GMI.P22_by_class()
#     GMI.P25_by_plan()
#     GMI.P26_by_class()
#     GMI.save(output_file='Filled file.xlsx')

# if __name__ == "__main__":
#     main()