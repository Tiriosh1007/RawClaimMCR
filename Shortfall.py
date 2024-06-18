import numpy as np
import pandas as pd
import datetime as dt
import io
import os
import warnings

warnings.filterwarnings('ignore')

class Shortfall():

  def __init__(self):

    col_setup = [
        'policy_id',
        'policy_number',
        'insurer',
        'client_name',
        'policy_start_date',
        'policy_end_date',
        'duration_days',
        'class',
        'benefit_type',
        'benefit',
        'panel',
        'no_of_claims',
        'no_of_claimants',
        'incurred_amount',
        'paid_amount',
    ]


    self.col_setup = col_setup
    self.df = pd.DataFrame(columns=self.col_setup)

    self.benefit_index = pd.read_excel('benefit_indexing.xlsx')



  def __bupa_shortfall(self, shortfall_fp):
    t_df = pd.read_excel(shortfall_fp, sheet_name='Report')
    client_name_ = t_df.iloc[:, 0].loc[t_df.iloc[:, 0].str.contains('Customer', case=False) == True].values[0].split(': ')[1].split('     ')[-1]
    policy_no_ = t_df.iloc[:, 0].loc[t_df.iloc[:, 0].str.contains('Contract', case=False) == True].values[0].split(': ')[1].split('     ')[-1]
    start_d_ = pd.to_datetime(t_df.iloc[:, 0].loc[t_df.iloc[:, 0].str.contains('Period', case=False) == True].values[0].split(': ')[1].split(' ')[-5], format='%Y-%m-%d')
    end_d_ = pd.to_datetime(t_df.iloc[:, 0].loc[t_df.iloc[:, 0].str.contains('Period', case=False) == True].values[0].split(': ')[1].split(' ')[-1], format='%Y-%m-%d')
    duration_ = (end_d_ - start_d_).days + 1
    policy_id_ = f'{policy_no_}_{start_d_:%Y%m}'

    t_df = pd.read_excel(shortfall_fp, sheet_name='Report', skiprows=9, dtype='str')

    __cols = ['no_of_claims',
          'no_of_claimants',
          'incurred_amount',
          'paid_amount',]

    if len(t_df.columns) > 10:
      t_df_non = t_df.iloc[:, 0:9].drop(columns=['Benefit']).dropna()
      t_df_non.columns = [
          'benefit_type',
          'class',
          'benefit',
          'no_of_claims',
          'no_of_claimants',
          'incurred_amount',
          'paid_amount',
          'usage_ratio',
      ]
      t_df_non['panel'] = 'Non-Panel'

      t_df_all = pd.concat([t_df.iloc[:, 0:4], t_df.iloc[:, 9:14]], axis=1).drop(columns=['Benefit']).dropna()
      t_df_all.columns= [
          'benefit_type',
          'class',
          'benefit',
          'no_of_claims',
          'no_of_claimants',
          'incurred_amount',
          'paid_amount',
          'usage_ratio',
      ]
      t_df_all['panel'] = 'Overall'

      t_df_pan = t_df_all.copy(deep=True)
      t_df_pan['panel'] = 'Panel'

      for col in __cols:
        t_df_pan[col] = t_df_pan[col].astype(float)
        t_df_all[col] = t_df_all[col].astype(float)
        t_df_non[col] = t_df_non[col].astype(float)
        t_df_pan[col] = t_df_all[col] - t_df_non[col]

      t_df = pd.concat([t_df_pan, t_df_non, t_df_all], axis=0, ignore_index=True)
    else:
      t_df_all = t_df.iloc[:, 0:9].drop(columns=['Benefit']).dropna()
      t_df_all.columns = [
          'benefit_type',
          'class',
          'benefit',
          'no_of_claims',
          'no_of_claimants',
          'incurred_amount',
          'paid_amount',
          'usage_ratio',
      ]
      t_df_all['panel'] = 'Overall'
      for col in __cols:
        t_df_all[col] = t_df_all[col].astype(float)

      t_df = t_df_all

    bone_pos_list = []
    for n00 in t_df.loc[t_df['benefit'].str.contains('Herbalist', case=False)].index.get_level_values(0):
      if n00 + 1 <= len(t_df.benefit):
        if t_df['benefit'].iloc[n00 +1] == 'Bonesetter':
          __no_of_claims = t_df['no_of_claims'].iloc[n00] + t_df['no_of_claims'].iloc[n00 + 1]
          t_df['no_of_claims'].loc[n00] = __no_of_claims 
          __no_of_claimants = t_df['no_of_claimants'].iloc[n00] + t_df['no_of_claimants'].iloc[n00 + 1]
          t_df['no_of_claimants'].loc[n00] = __no_of_claimants
          __incurred = t_df['incurred_amount'].iloc[n00] + t_df['incurred_amount'].iloc[n00 + 1]
          t_df['incurred_amount'].loc[n00] = __incurred
          __paid = t_df['paid_amount'].iloc[n00] + t_df['paid_amount'].iloc[n00 + 1]
          t_df['paid_amount'].loc[n00] = __paid
          bone_pos_list.append(n00+1)
    if len(bone_pos_list) > 0:
      t_df.drop(index=bone_pos_list, inplace=True)

    t_df['policy_id'] = policy_id_
    t_df['policy_number'] = policy_no_
    t_df['insurer'] = 'Bupa'
    t_df['client_name'] = client_name_
    t_df['policy_start_date'] = start_d_
    t_df['policy_end_date'] = end_d_
    t_df['duration_days'] = duration_
    t_df['benefit_type'].replace({'Clinical': 'Clinic'}, inplace=True)

    bupa_index = self.benefit_index[['gum_benefit', 'bupa_benefit_desc']]
    t_df = pd.merge(left=t_df, right=bupa_index, left_on='benefit', right_on='bupa_benefit_desc', how='left')
    t_df.benefit = t_df.gum_benefit

    t_df = t_df[self.col_setup]

    return t_df

  def add_shortfall(self, shortfall_fp, insurer='Bupa'):
    if insurer == 'Bupa':
      t_df = self.__bupa_shortfall(shortfall_fp)


    self.df = pd.concat([self.df, t_df], axis=0, ignore_index=True)
    return
  
  def remove_overall(self):
    self.full_df = self.df
    self.df = self.df.loc[self.df.panel != 'Overall']
    return

  def mcr_p20_policy(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p20_policy_df = self.df[['policy_number', 'year', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year'], dropna=False).sum()
      p20_policy_df['usage_ratio'] = p20_policy_df['paid_amount'] / p20_policy_df['incurred_amount']
      p20_policy_df = p20_policy_df.unstack().stack(dropna=False)
      self.p20_policy = p20_policy_df
    return p20_policy_df

  def mcr_p20_benefit(self, by=None, benefit_type_order=['Hospital', 'Clinic', 'Dental', 'Optical', 'Maternity', 'Total']):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p20_benefit_df = self.df[['policy_number', 'year', 'benefit_type', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year', 'benefit_type'], dropna=False).sum()
      for __policy_number in p20_benefit_df.index.get_level_values(0).unique():
        for __year in p20_benefit_df.index.get_level_values(1).unique():
            p20_benefit_df.loc[__policy_number, __year, 'Total'] = p20_benefit_df.loc[__policy_number, __year, :].sum()
            # print(p20_benefit_df.loc[__policy_number, :].loc[__year, :].sum())

      p20_benefit_df['usage_ratio'] = p20_benefit_df['paid_amount'] / p20_benefit_df['incurred_amount']
      p20_benefit_df = p20_benefit_df.unstack().stack(dropna=False)
      p20_benefit_df = p20_benefit_df.reindex(benefit_type_order, level='benefit_type')
      self.p20_benefit = p20_benefit_df
      self.p20 = p20_benefit_df
    return p20_benefit_df

  def mcr_p20_panel(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p20_panel_df = self.df[['policy_number', 'year', 'panel', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year', 'panel'], dropna=False).sum()
      p20_panel_df['usage_ratio'] = p20_panel_df['paid_amount'] / p20_panel_df['incurred_amount']
      p20_panel_df = p20_panel_df.unstack().stack(dropna=False)
      self.p20_panel = p20_panel_df
    return p20_panel_df

  def mcr_p21_class(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p21_class_df = self.df[['policy_number', 'year', 'class', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year', 'class'], dropna=False).sum()
      p21_class_df['usage_ratio'] = p21_class_df['paid_amount'] / p21_class_df['incurred_amount']
      p21_class_df = p21_class_df.unstack().stack(dropna=False)
      self.p21_class = p21_class_df
      self.p21 = p21_class_df
    return p21_class_df

  def mcr_p22_class_benefit(self, by=None, benefit_type_order=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p22_class_benefit_df = self.df[['policy_number', 'year', 'class', 'benefit_type', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year', 'class', 'benefit_type'], dropna=False).sum()
      p22_class_benefit_df['usage_ratio'] = p22_class_benefit_df['paid_amount'] / p22_class_benefit_df['incurred_amount']
      p22_class_benefit_df = p22_class_benefit_df.unstack().stack(dropna=False)
      p22_class_benefit_df = p22_class_benefit_df.reindex(benefit_type_order, level='benefit_type')
      self.p22_class_benefit = p22_class_benefit_df
      self.p22 = p22_class_benefit_df
    return p22_class_benefit_df

  def mcr_p23_ip_benefit(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p23_ip_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p23_ip_benefit_df['usage_ratio'] = p23_ip_benefit_df['paid_amount'] / p23_ip_benefit_df['incurred_amount']
      # p23_ip_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      # p23_ip_benefit_df['no_of_claims'] = p23_ip_no_claims['no_of_claims']
      p23_ip_benefit_df = p23_ip_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p23_ip_benefit_df = p23_ip_benefit_df.unstack().stack(dropna=False)
      p23_ip_benefit_df = p23_ip_benefit_df.reindex(self.ip_order, level='benefit')
      self.p23_ip_benefit = p23_ip_benefit_df
      self.p23 = p23_ip_benefit_df
    return p23_ip_benefit_df

  def mcr_p23a_class_ip_benefit(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p23_ip_class_benefit_df = self.df[['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).sum()
      p23_ip_class_benefit_df['usage_ratio'] = p23_ip_class_benefit_df['paid_amount'] / p23_ip_class_benefit_df['incurred_amount']
      # p23_ip_class_no_claims = self.df[['policy_number', 'year', 'class', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      # p23_ip_class_benefit_df['no_of_claims'] = p23_ip_class_no_claims['no_of_claims']
      p23_ip_class_benefit_df = p23_ip_class_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p23_ip_class_benefit_df = p23_ip_class_benefit_df.unstack().stack(dropna=False)
      p23_ip_class_benefit_df = p23_ip_class_benefit_df.reindex(self.ip_order, level='benefit')
      self.p23a_ip_class_benefit = p23_ip_class_benefit_df
      self.p23a = p23_ip_class_benefit_df
    return p23_ip_class_benefit_df

  def mcr_p24_op_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p24_op_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p24_op_benefit_df['usage_ratio'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['incurred_amount']
      # p24_op_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      # p24_op_benefit_df['no_of_claims'] = p24_op_no_claims['no_of_claims']
      p24_op_benefit_df = p24_op_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p24_op_benefit_df['incurred_per_claim'] = p24_op_benefit_df['incurred_amount'] / p24_op_benefit_df['no_of_claims']
      p24_op_benefit_df['paid_per_claim'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['no_of_claims']
      # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
      p24_op_benefit_df = p24_op_benefit_df.unstack().stack(dropna=False)
      # p24_op_benefit_df.sort_values(by=['policy_number', 'year', 'paid_amount'], ascending=[True, True, False], inplace=True)
      self.p24_op_benefit = p24_op_benefit_df
      self.p24 = p24_op_benefit_df
    return p24_op_benefit_df

  def mcr_p24a_op_class_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      print(self.df.columns)
      p24_op_class_benefit_df = self.df[['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).sum()
      p24_op_class_benefit_df['usage_ratio'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['incurred_amount']
      # p24_op_class_no_claims = self.df[['policy_number', 'year', 'class', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      # p24_op_class_benefit_df['no_of_claims'] = p24_op_class_no_claims['no_of_claims']
      p24_op_class_benefit_df = p24_op_class_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p24_op_class_benefit_df['incurred_per_claim'] = p24_op_class_benefit_df['incurred_amount'] / p24_op_class_benefit_df['no_of_claims']
      p24_op_class_benefit_df['paid_per_claim'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['no_of_claims']
      # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
      p24_op_class_benefit_df = p24_op_class_benefit_df.unstack().stack(dropna=False)
      # p24_op_class_benefit_df.sort_values(by=['policy_number', 'year', 'class', 'paid_amount'], ascending=[True, True, True, False], inplace=True)
      self.p24a_op_class_benefit = p24_op_class_benefit_df
      self.p24a = p24_op_class_benefit_df
    return p24_op_class_benefit_df

  def mcr_p24_dent_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p24_dent_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Dental'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p24_dent_benefit_df['usage_ratio'] = p24_dent_benefit_df['paid_amount'] / p24_dent_benefit_df['incurred_amount']
      # p24_dent_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Dental'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      # p24_dent_benefit_df['no_of_claims'] = p24_dent_no_claims['no_of_claims']
      p24_dent_benefit_df = p24_dent_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p24_dent_benefit_df['incurred_per_claim'] = p24_dent_benefit_df['incurred_amount'] / p24_dent_benefit_df['no_of_claims']
      p24_dent_benefit_df['paid_per_claim'] = p24_dent_benefit_df['paid_amount'] / p24_dent_benefit_df['no_of_claims']
      # p24_dent_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
      # print(p24_dent_benefit_df)
      p24_dent_benefit_df = p24_dent_benefit_df.unstack().stack(dropna=False)
      if len(p24_dent_benefit_df) > 0:
        p24_dent_benefit_df.sort_values(by=['policy_number', 'year', 'paid_amount'], ascending=[True, True, False], inplace=True)
      self.p24_dent_benefit = p24_dent_benefit_df
    return p24_dent_benefit_df

  def mcr_p25_class_panel_benefit(self, by=None, benefit_type_order=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p25_class_panel_benefit_df = self.df[['policy_number', 'year', 'class', 'panel', 'benefit_type', 'incurred_amount', 'paid_amount']].groupby(by=['policy_number', 'year', 'class', 'panel', 'benefit_type'], dropna=False).sum()
      p25_class_panel_benefit_df['usage_ratio'] = p25_class_panel_benefit_df['paid_amount'] / p25_class_panel_benefit_df['incurred_amount']
      p25_class_panel_benefit_df = p25_class_panel_benefit_df.unstack().stack(dropna=False)
      p25_class_panel_benefit_df = p25_class_panel_benefit_df.reindex(benefit_type_order, level='benefit_type')
      self.p25_class_panel_benefit = p25_class_panel_benefit_df
      self.p25 = p25_class_panel_benefit_df
    return p25_class_panel_benefit_df

  def mcr_p26_op_panel(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p26_op_panel_df = self.df[['policy_number', 'year', 'panel', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'panel', 'benefit'], dropna=False).sum()
      p26_op_panel_df['usage_ratio'] = p26_op_panel_df['paid_amount'] / p26_op_panel_df['incurred_amount']
      # p26_op_no_claims = self.df[['policy_number', 'year', 'panel', 'benefit', 'paid_amount']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'panel', 'benefit'], dropna=False).count().rename(columns={'paid_amount': 'no_of_claims'})
      # p26_op_panel_df['no_of_claims'] = p26_op_no_claims['no_of_claims']
      p26_op_panel_df = p26_op_panel_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p26_op_panel_df['incurred_per_claim'] = p26_op_panel_df['incurred_amount'] / p26_op_panel_df['no_of_claims']
      p26_op_panel_df['paid_per_claim'] = p26_op_panel_df['paid_amount'] / p26_op_panel_df['no_of_claims']
      p26_op_panel_df = p26_op_panel_df.unstack().stack(dropna=False)
      if len(p26_op_panel_df.index) > 0:
        p26_op_panel_df.sort_values(by=['policy_number', 'year', 'panel', 'paid_amount'], ascending=[True, True, True, False], inplace=True)
      self.p26_op_panel = p26_op_panel_df
      self.p26 = p26_op_panel_df
    return p26_op_panel_df

  def mcr_p27_class_dep_op_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p27_df = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incurred_amount', 'paid_amount', 'no_of_claims']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).sum()
      p27_df['usage_ratio'] = p27_df['paid_amount'] / p27_df['incurred_amount']
      # if bupa == False:
      #   p27_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'paid_amount']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'paid_amount': 'no_of_claims'})
      #   p27_df['no_of_claims'] = p27_df_claims['no_of_claims']
      # else:
      # p27_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incur_date']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'incur_date': 'no_of_claims'})
      # p27_df['no_of_claims'] = p27_df_claims['no_of_claims']
      p27_df = p27_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims']]
      p27_df['incurred_per_claim'] = p27_df['incurred_amount'] / p27_df['no_of_claims']
      p27_df['paid_per_claim'] = p27_df['paid_amount'] / p27_df['no_of_claims']
      p27_df = p27_df.unstack().stack(dropna=False)
      p27_df.sort_values(by=['policy_number', 'year', 'class', 'dep_type', 'paid_amount'], ascending=[True, True, True, True, False], inplace=True)
      self.p27 = p27_df
    return p27_df



  def mcr_pages(self, by=None, export=False, benefit_type_order=['Hospital', 'Clinic', 'Dental', 'Optical', 'Maternity', 'Total']):
    
    self.mcr_p20_policy(by)
    self.mcr_p20_benefit(by, benefit_type_order)
    self.mcr_p20_panel(by)
    self.mcr_p21_class(by)
    self.mcr_p22_class_benefit(by, benefit_type_order)
    self.mcr_p23_ip_benefit(by)
    self.mcr_p23a_class_ip_benefit(by)
    self.mcr_p24_op_benefit(by)
    self.mcr_p24a_op_class_benefit(by)
    self.mcr_p24_dent_benefit(by)
    self.mcr_p25_class_panel_benefit(by, benefit_type_order)
    self.mcr_p26_op_panel(by)
    # self.mcr_p27_class_dep_op_benefit(by)

    if export == True:
      from io import BytesIO
      output = BytesIO()
      # mcr_filename = 'mcr.xlsx'
      mcr_filename = output
      with pd.ExcelWriter(mcr_filename) as writer:
        self.p20_policy.to_excel(writer, sheet_name='P.20_Policy', index=True, merge_cells=False)
        self.p20.to_excel(writer, sheet_name='P.20_BenefitType', index=True, merge_cells=False)
        self.p20_panel.to_excel(writer, sheet_name='P.20_Network', index=True, merge_cells=False)
        self.p21.to_excel(writer, sheet_name='P.21_Class', index=True, merge_cells=False)
        self.p22.to_excel(writer, sheet_name='P.22_Class_BenefitType', index=True, merge_cells=False)
        self.p23.to_excel(writer, sheet_name='P.23_IP_Benefit', index=True, merge_cells=False)
        self.p23a.to_excel(writer, sheet_name='P.23a_Class_IP_Benefit', index=True, merge_cells=False)
        self.p24.to_excel(writer, sheet_name='P.24_OP_Benefit', index=True, merge_cells=False)
        self.p24a.to_excel(writer, sheet_name='P.24a_Class_OP_Benefit', index=True, merge_cells=False)
        self.p24_dent_benefit.to_excel(writer, sheet_name='P.24b_DentalBenefit', index=True, merge_cells=False)
        self.p25.to_excel(writer, sheet_name='P.25_Class_Panel_BenefitType', index=True, merge_cells=False)
        self.p26.to_excel(writer, sheet_name='P.26_OP_Panel_Benefit', index=True, merge_cells=False)
        # self.p27.to_excel(writer, sheet_name='P.27_Class_Dep_OP_Benefit', index=True, merge_cells=False)
        writer.close()
        # processed_data = output.getvalue()
        return output.getvalue()
      
  def export_database(self):
    return self.full_df.to_csv(index=False).encode('utf-8')
