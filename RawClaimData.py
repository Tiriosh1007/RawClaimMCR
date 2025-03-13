
import pandas as pd
import numpy as np
import os
import io
import msoffcrypto
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import datetime as dt
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')
sns.set(rc={'figure.figsize':(5,5)})
plt.rcParams["axes.formatter.limits"] = (-99, 99)

from ColumnNameManagement import *

class RawClaimData():

  def __init__(self, benefit_index_path):
    col_setup = [
        'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'policy_number',
        'insurer',
        'client_name',
        'policy_start_date',
        'policy_end_date',
        'claim_id',
        'incur_date',
        'discharge_date',
        'submission_date',
        'pay_date',
        'claim_status',
        'claim_remark',
        'cert_true_copy',
        'claimant',
        'gender',
        'age',
        'dep_type',
        'class',
        'member_status',
        'benefit_type',
        'benefit',
        'diagnosis',
        'procedure',
        'hospital_name',
        'provider',
        'physician',
        'chronic',
        'currency',
        'incurred_amount',
        'paid_amount',
        'panel',
        'suboffice',
        'region',
    ]

    self.col_setup = col_setup
    self.df = pd.DataFrame(columns=self.col_setup)
    self.upload_time = dt.datetime.now()
    self.upload_log = pd.DataFrame(columns=['policy_id', 'upload_time', 'insurer', 'shortfall_supplement'])
    self.benefit_index = pd.read_excel('benefit_indexing.xlsx')



  def __axa_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):

    axa_df = pd.DataFrame(columns=self.col_setup)


    rename_col_clin = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY HOLDER NO': 'policy_number',
        # 'insurer', # AXA
        # 'client_name', # self input
        # 'policy_start_date', # from file name
        # 'policy_end_date', # from file name
        'CLAIM NUMBER': 'claim_id',
        'DATE OF ADMISSION': 'incur_date',
        # 'discharge_date', # clinic set to incur_date
        'DATE OF RECEIPT': 'submission_date',
        'DATE OF PAYMENT': 'pay_date',
        'STATUS': 'claim_status',
        'REMARK CODE1': 'claim_remark',
        # 'cert_true_copy', # deduce from remark code
        'MEMBER': 'claimant',
        'CERTIFICATE NO': 'claimant',
        'SEX': 'gender',
        # 'age', deduce from DOB
        'DEPENDENT NO': 'dep_type',
        'HEALTH CLASS LEVEL  /OPT. HOSPITAL CLASS': 'class',
        # 'member_status', # not available
        # 'benefit_type', # sheetname
        'BENEFIT CODE': 'benefit', # need process
        # 'diagnosis', # not available
        # 'chronic', # not available
        'CURRENCY': 'currency',
        'AMOUNT BILLED': 'incurred_amount',
        'AMOUNT PAID': 'paid_amount',
        # 'panel', deduce from benefit code
        'AFFILIATED CO': 'suboffice',
        # 'region',
        'AGE': 'age',
        'DIAGNOSIS': 'diagnosis',

        'DATE OF BIRTH': 'birth_date'
    }

    dtype_axa_clin = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY HOLDER NO': str,
        # 'insurer', # AXA
        # 'client_name', # self input
        # 'policy_start_date', # from file name
        # 'policy_end_date', # from file name
        'CLAIM NUMBER': str,
        'DATE OF ADMISSION': str,
        # 'discharge_date', # clinic set to 1
        'DATE OF RECEIPT': str,
        'DATE OF PAYMENT': str,
        'STATUS': str,
        'REMARK CODE1': str,
        # 'cert_true_copy', # deduce from remark code
        'MEMBER': str,
        'CERTIFICATE NO': str,
        'SEX': str,
        # 'age', deduce from DOB
        'DEPENDENT NO': str,
        'HEALTH CLASS LEVEL  /OPT. HOSPITAL CLASS': str,
        # 'member_status', # not available
        # 'benefit_type', # sheetname
        'BENEFIT CODE': str, # need process
        # 'diagnosis', # not available
        # 'chronic', # not available
        'CURRENCY': str,
        'AMOUNT BILLED': float,
        'AMOUNT PAID': float,
        # 'panel', deduce from benefit code
        'AFFILIATED CO': str,
        # 'region',
        'AGE': int,
        'DIAGNOSIS': str,

        'DATE OF BIRTH': str
    }

    date_col_clin = ['incur_date', 'submission_date', 'pay_date', 'birth_date']

    rename_col_hosp = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY HOLDER NO': 'policy_number',
        # 'insurer', # AXA
        # 'client_name', # self input
        # 'policy_start_date', # from file name
        # 'policy_end_date', # from file name
        'CLAIM NUMBER': 'claim_id',
        'DATE OF ADMISSION': 'incur_date',
        'DATE OF DISCHARGE': 'discharge_date',
        'DATE OF RECEIPT': 'submission_date',
        'DATE OF PAYMENT': 'pay_date',
        'STATUS': 'claim_status',
        'REMARK': 'claim_remark',
        # 'cert_true_copy', # deduce from remark code
        'MEMBER': 'claimant',
        'CERTIFICATE NO': 'claimant',
        'SEX': 'gender',
        # 'age', deduce from DOB
        'DEPENDENT NO': 'dep_type',
        'HEALTH CLASS LEVEL  /OPT. HOSPITAL CLASS': 'class',
        # 'member_status', # not available
        # 'benefit_type', # sheetname
        'BENEFIT CODE': 'benefit', # need process
        # 'diagnosis', # not available
        # 'chronic', # not available
        'CURRENCY': 'currency',
        'AMOUNT BILLED': 'incurred_amount',
        'AMOUNT PAID': 'paid_amount',
        # 'panel', deduce from benefit code
        'AFFILIATED CO': 'suboffice',
        # 'region',
        'AGE': 'age',
        'DIAGNOSIS': 'diagnosis',

        'DATE OF BIRTH': 'birth_date'
    }

    dtype_axa_hosp = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY HOLDER NO': str,
        # 'insurer', # AXA
        # 'client_name', # self input
        # 'policy_start_date', # from file name
        # 'policy_end_date', # from file name
        'CLAIM NUMBER': str,
        'DATE OF ADMISSION': str,
        'DATE OF DISCHARGE': str,
        'DATE OF RECEIPT': str,
        'DATE OF PAYMENT': str,
        'STATUS': str,
        'REMARK': str,
        # 'cert_true_copy', # deduce from remark code
        'MEMBER': str,
        'CERTICFICATE NO': str,
        'SEX': str,
        # 'age', deduce from DOB
        'DEPENDENT NO': str,
        'HEALTH CLASS LEVEL  /OPT. HOSPITAL CLASS': str,
        # 'member_status', # not available
        # 'benefit_type', # sheetname
        'BENEFIT CODE': str, # need process
        # 'diagnosis', # not available
        # 'chronic', # not available
        'CURRENCY': str,
        'AMOUNT BILLED': float,
        'AMOUNT PAID': float,
        # 'panel', deduce from benefit code
        'AFFILIATED CO': str,
        # 'region',
        'AGE': int,
        'DIAGNOSIS': str,

        'DATE OF BIRTH': str
    }
    date_col_hosp = ['incur_date', 'discharge_date', 'submission_date', 'pay_date', 'birth_date']

    unlocked_file = io.BytesIO()

    _policy_number = raw_claim_path.split('/')[-1].split('_')[1]
    _policy_start = raw_claim_path.split('/')[-1].split('_')[3]
    _policy_end = raw_claim_path.split('/')[-1].split('_')[4].split(' - ')[0]
    _client_name = client_name
    _insurer = 'AXA'

    print(_policy_start)

    with open(raw_claim_path, "rb") as file:
      if password in not None:
        excel_file = msoffcrypto.OfficeFile(file)
        excel_file.load_key(password = password)
        excel_file.decrypt(unlocked_file)
      from openpyxl import load_workbook
      wb = load_workbook(filename = unlocked_file)

      b_type_l = wb.sheetnames

      if 'CLIN' in b_type_l:
        df_c = pd.read_excel(unlocked_file, sheet_name='CLIN', dtype=dtype_axa_clin)

        # length = length + len(df_c)
        # print(len(df_c))
        df_c.rename(columns=rename_col_clin, inplace=True)


        if len(df_c) > 0:
          for col in date_col_clin:
            if col in df_c.columns.tolist():
              df_c[col] = df_c[col].loc[df_c[col] != '0']
              df_c[col] = pd.to_datetime(df_c[col], format='%Y%m%d')
            else:
              df_c[col] = np.nan



          if policy_start_date == None:
            start_d_ = pd.to_datetime(_policy_start, format='%Y%m%d')
          else:
            start_d_ = pd.to_datetime(policy_start_date, format='%Y%m%d')

          policy_no_ = df_c['policy_number'].iloc[0]
          df_c['policy_id'] = f'{policy_no_}_{start_d_:%Y%m}'

          df_c['insurer'] = 'AXA'
          df_c['client_name'] = client_name
          df_c['policy_start_date'] = start_d_
          df_c['policy_end_date'] = start_d_ + pd.DateOffset(years=1)
          # df_c['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          df_c['discharge_date'] = df_c['incur_date']
          df_c['cert_true_copy'] = np.nan
          print(df_c['birth_date'].values[0])
          if 'age' in df_c.columns.tolist() and df_c['age'].values[0] != np.nan:
            df_c['age'] = df_c['age'].astype(int)
          elif pd.isna(df_c['birth_date'].values[0]) == True:
            df_c['age'] = np.nan
          elif 'birth_date' in df_c.columns.tolist() and df_c['birth_date'].values[0] != np.nan:
            df_c['age'] = ((df_c['policy_start_date'] - df_c['birth_date']).dt.days/365.25).astype(int)
          else:
            df_c['age'] = np.nan
          df_c['member_status'] = np.nan
          df_c['benefit_type'] = 'Clinic'
          df_c['diagnosis'] = np.nan
          df_c['chronic'] = np.nan
          df_c['panel'] = 'Non-Panel'
          df_c['panel'].loc[df_c.benefit.str.contains('1')] = 'Panel'

        for col in self.col_setup:
          if col not in df_c.columns.tolist():
            df_c[col] = np.nan


      if 'DENT' in b_type_l:
        df_d = pd.read_excel(unlocked_file, sheet_name='DENT', dtype=dtype_axa_clin)
        df_d.rename(columns=rename_col_clin, inplace=True)
        if len(df_d) > 0:
          for col in date_col_clin:
            if col in df_d.columns.tolist():
              df_d[col] = pd.to_datetime(df_d[col], format='%Y%m%d')
            else:
              df_d[col] = np.nan

          if policy_start_date == None:
            start_d_ = pd.to_datetime(_policy_start, format='%Y%m%d')
          else:
            start_d_ = pd.to_datetime(policy_start_date, format='%Y%m%d')

          policy_no_ = df_d['policy_number'].iloc[0]
          df_d['policy_id'] = f'{policy_no_}_{start_d_:%Y%m}'

          df_d['insurer'] = 'AXA'
          df_d['client_name'] = client_name
          df_d['policy_start_date'] = start_d_
          df_d['policy_end_date'] = start_d_ + pd.DateOffset(years=1)
          #df_d['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          df_d['discharge_date'] = df_d['incur_date']
          df_d['cert_true_copy'] = np.nan
          if 'age' in df_d.columns.to_list() and df_d['age'].values[0] != np.nan:
            df_d['age'] = df_d['age'].astype(int)
          elif pd.isna(df_d['birth_date'].values[0]) == True:
            df_d['age'] = np.nan
          elif 'birth_date' in df_d.columns.tolist() and df_d['birth_date'].values[0] != np.nan:
            df_d['age'] = ((df_d['policy_start_date'] - df_d['birth_date']).dt.days/365.25).astype(int)
          else:
            df_d['age'] = np.nan
          df_d['member_status'] = np.nan
          df_d['benefit_type'] = 'Dental'
          df_d['diagnosis'] = np.nan
          df_d['chronic'] = np.nan
          df_d['panel'] = 'Non-Panel'
          df_d['panel'].loc[df_d.benefit.str.contains('1')] = 'Panel'

        for col in self.col_setup:
          if col not in df_d.columns.tolist():
            df_d[col] = np.nan


      if 'HOSP' in b_type_l:

        df_h = pd.read_excel(unlocked_file, sheet_name='HOSP', dtype=dtype_axa_hosp)
        df_h.rename(columns=rename_col_hosp, inplace=True)
        if len(df_h) > 0:
          for col in date_col_hosp:
            if col in df_h.columns.tolist():
              df_h[col] = pd.to_datetime(df_h[col], format='%Y%m%d')
            else:
              df_h[col] = np.nan

          if policy_start_date == None:
            start_d_ = pd.to_datetime(_policy_start, format='%Y%m%d')
          else:
            start_d_ = pd.to_datetime(policy_start_date, format='%Y%m%d')

          policy_no_ = df_h['policy_number'].iloc[0]
          df_h['policy_id'] = f'{policy_no_}_{start_d_:%Y%m}'

          df_h['insurer'] = 'AXA'
          df_h['client_name'] = client_name
          df_h['policy_start_date'] = start_d_
          df_h['policy_end_date'] = start_d_ + pd.DateOffset(years=1)
          # df_h['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          # df_h['discharge_date'] = df_h['incur_date']
          df_h['cert_true_copy'] = np.nan

          if 'age' in df_h.columns.to_list() and df_h['age'].values[0] != np.nan:
            df_h['age'] = df_h['age'].astype(int)
          elif pd.isna(df_h['birth_date'].values[0]) == True:
            df_h['age'] = np.nan
          elif 'birth_date' in df_h.columns.tolist() and df_h['birth_date'].values[0] != np.nan:
            df_h['age'] = ((df_h['policy_start_date'] - df_h['birth_date']).dt.days/365.25).astype(int)
          else:
            df_h['age'] = np.nan

          df_h['member_status'] = np.nan
          df_h['benefit_type'] = 'Hospital'
          df_h['diagnosis'] = np.nan
          df_h['chronic'] = np.nan
          df_h['panel'] = 'Non-Panel'
          df_h['panel'].loc[df_h.benefit.str.contains('1')] = 'Panel'

        for col in self.col_setup:
          if col not in df_h.columns.tolist():
            df_h[col] = np.nan

      axa_index = self.benefit_index[['gum_benefit', 'axa_benefit_code']]
      # print(df_d.columns)
      t_df = pd.concat([df_c[self.col_setup], df_h[self.col_setup], df_d[self.col_setup]], axis=0, ignore_index=True)
      t_df = pd.merge(left=t_df, right=axa_index, left_on='benefit', right_on='axa_benefit_code', how='left')
      t_df.benefit = t_df.gum_benefit
      t_df.suboffice.fillna('00', inplace=True)
      t_df.diagnosis.fillna('No diagnosis provided', inplace=True)
      t_df['region'] = region
      t_df = t_df[self.col_setup]

    if pd.isna(t_df.gender).any() == False:
      t_df.gender.loc[t_df.gender.str.contains('f', case=False)] = 'F'
      t_df.gender.loc[t_df.gender.str.contains('m', case=False)] = 'M'

    axa_dep_type = {'1': 'EE', '2': 'SP', '3': 'CH', '4': 'CH', '5': 'CH', '6': 'CH', '7': 'CH', '8': 'CH'}
    t_df.dep_type.replace(axa_dep_type, inplace=True)
    # self.df = pd.concat([self.df, t_df], axis=0)
    return t_df

  def __bupa_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      bupa_rename_col = {
          # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
          # 'policy_number', # Contract NUmber A1
          # 'insurer', # Bupa
          # 'client_name', # Customer Name A2
          # 'policy_start_date', # Period Cell A3
          # 'policy_end_date',
          'Voucher number': 'claim_id',
          'Voucher Number': 'claim_id',
          'Incur Date': 'incur_date',
          'Incurred From Date': 'incur_date',
          # 'discharge_date', convert from days cover
          'Receipt date': 'submission_date',
          'Pay date': 'pay_date',
          'Pay Date': 'pay_date',
          'Status Code': 'claim_status',
          'Reject Code Description': 'claim_remark',
          # 'cert_true_copy',
          'Claimant': 'claimant',
          'Sex': 'gender',
          'Age': 'age',
          'Member type': 'dep_type',
          'Member Type': 'dep_type',
          'Class': 'class',
          'Class ID': 'class',
          'Member Status': 'member_status',
          'Benefit Type': 'benefit_type',
          'Benefit': 'benefit',
          'Benefit Code Description': 'benefit',
          'Diagnosis description': 'diagnosis',
          'Diagnosis': 'diagnosis',
          'Procedure Description': 'procedure',
          'Hospital Name': 'hospital_name',
          'Provider Name': 'provider',
          'Physician Name': 'physician',
          'Chronic flag': 'chronic',
          'Diagnosis Chronic Flag': 'cheonic',
          # 'currency',
          'Presented': 'incurred_amount',
          'Adjusted': 'paid_amount',
          'Network Category': 'panel',
          'Network': 'panel',
          
          # 'suboffice', # deduced from contract number columns last 2 digits
          # 'region',

          'Days Cover': 'days_cover',
          'Contract Number': 'contract_number',
      }
      dtype_bupa = {
          # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
          # 'policy_number', # Contract NUmber A1
          # 'insurer', # Bupa
          # 'client_name', # Customer Name A2
          # 'policy_start_date', # Period Cell A3
          # 'policy_end_date',
          'Voucher number': str,
          'Voucher Number': str,
          'Incur Date': str,
          'Incurred From Date': str,
          # 'discharge_date', convert from days cover
          'Receipt date': str,
          'Pay date': str,
          'Pay Date': str,
          'Status Code': str,
          # 'claim_status',
          # 'claim_remark',
          'Reject Code Description': str,
          # 'cert_true_copy',
          'Claimant': str,
          'Sex': str,
          'Age': int,
          'Member type': str,
          'Member Type': str,
          'Class': str,
          'Class ID': str,
          'Member Status': str,
          'Benefit Type': str,
          'Benefit': str,
          'Benefit Code Description': str,
          'Diagnosis description': str,
          'Diagnosis': str,
          'Procedure Description': str,
          'Hospital Name': str,
          'Chronic flag': str,
          'Diagnosis Chronic Flag': str,
          # 'currency',
          'Presented': float,
          'Adjusted': float,
          'Network Category': str,
          'Network': str,
          # 'suboffice', # deduced from contract number columns last 2 digits
          # 'region',

          'Days Cover': int,
          'Contract Number': str,
      }

    date_cols = ['incur_date', 'submission_date', 'pay_date']
    df_ = pd.read_excel(raw_claim_path, sheet_name='Details')
    client_ = df_.columns[0].split('   ')[-1]
    policy_no_ = str(df_.iloc[0, 0].split('   ')[-1])
  

    if policy_start_date != None:
      start_d_ = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      start_d_ = pd.to_datetime(df_.iloc[1, 0].split(': ')[1].split(' ')[-5], format='%Y-%m-%d')
    end_d_ = pd.to_datetime(df_.iloc[1, 0].split(': ')[1].split(' ')[-1], format='%Y-%m-%d')
    df_ = pd.read_excel(raw_claim_path, sheet_name='Details', skiprows=7, dtype=dtype_bupa)
    df_.rename(columns=bupa_rename_col, inplace=True)

    for col in date_cols:
      if col in df_.columns.tolist():
        df_[col] = df_[col].loc[df_[col] != '0']
        df_[col] = pd.to_datetime(df_[col], format='%Y%m%d')
      else:
        df_[col] = np.nan

    if client_name != None:
      df_['client_name'] = client_name
    else:
      df_['client_name'] = client_

    df_['policy_number'] = policy_no_
    df_['policy_id'] = f'{policy_no_}_{start_d_:%Y%m}'
    df_['insurer'] = 'Bupa'
    df_['policy_start_date'] = start_d_
    df_['policy_end_date'] = end_d_
    df_['discharge_date'] = df_['incur_date']

    if 'days_cover' in df_.columns:
      df_['discharge_date'].loc[df_.benefit_type == 'H'] = df_['discharge_date'] + pd.to_timedelta(df_.days_cover, unit='D')
    df_['suboffice'] = df_['contract_number'].str[-2:]

    df_['region'] = region

    bupa_index = self.benefit_index[['gum_benefit', 'bupa_benefit_desc']]
    df_ = pd.merge(left=df_, right=bupa_index, left_on='benefit', right_on='bupa_benefit_desc', how='left')
    df_.benefit = df_.gum_benefit

    bupa_b_type = {'C': 'Clinic', 'H': 'Hospital', 'D': 'Dental', 'G': 'Optical', 'T': 'Hospital', 'W': 'Hospital', 'M': 'Maternity'}
    df_['benefit_type'].replace(bupa_b_type, inplace=True)
    bupa_dep_type = {'E': 'EE', 'S': 'SP', 'C': 'CH'}
    df_['dep_type'].replace(bupa_dep_type, inplace=True)
    bupa_panel_type = {'H': 'Panel', 'N': 'Non-Panel', 'O': 'Panel'}
    df_['panel'].replace(bupa_panel_type, inplace=True)
    df_.gender.loc[df_.gender.str.contains('f', case=False)] = 'F'
    df_.gender.loc[df_.gender.str.contains('m', case=False)] = 'M'
    df_.diagnosis.fillna('No diagnosis provided', inplace=True)

    for col in self.col_setup:
      if col not in df_.columns:
        df_[col] = np.nan

    df_ = df_[self.col_setup]



    return df_

  def __aia_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      dtype_aia = {
          # 'policy_id',
          'Policy No': str,
          'Policy': str,
          # 'insurer',
          'client_name': str,
          'Plan Start Date': str,
          'Plan End Date': str,
          'Claim ID': str,
          'ClaimNo.': str,
          'Claim No.': str,
          'Admission Date': str,
          'Incurred Date': str,
          'Date From': str,
          'Discharge date': str,
          'File Date': str,
          'Received Date': str,
          'Date Notified': str,
          'Claim Payment Date': str,
          'Process Date': str,
          # 'claim_status',
          'Remark1': str,
          # 'cert_true_copy',
          'Member ID': str,
          'Claimant No.': str,
          'Claimant Name': str,
          'Gender': str,
          'Sex': str, # M/F
          'Age': str,
          'Relationships': str, # MEMBER/ SPOUSE/ CHILD
          'Dep Type': str, # M/ S/ C
          'Plan': str,
          'BenPlan': str,
          # 'member_status',
          'Claim Type': str,
          'Product Name': str,
          'Claim Type 2': str,
          'Ben Desc': str,
          'Diagnosis': str,
          # 'chronic',
          'Billed Currency': str,
          'Currency Symbol': str,
          'Submitted Claim Amount': float,
          'Presented Amount': float,
          'Amount Billed': float,
          'Paid Claim': float,
          'Reimbursed Amount': float,
          'Amount Paid': float,
          'Provider Type': str,
          'Network/Non-network': str,
          'SubOffice': str,
          # 'Entity': str,
          # 'region',
      }
      aia_rename_col = {
          # 'policy_id',
          'Policy No': 'policy_number',
          'Policy': 'policy_number',
          # 'insurer',
          'client_name': 'Client Name',
          'Plan Start Date': 'policy_start_date',
          'Plan End Date': 'policy_end_date',
          'Claim ID': 'claim_id',
          'ClaimNo.': 'claim_id',
          'Claim No.': 'claim_id',
          'Admission Date': 'incur_date',
          'Incurred Date': 'incur_date',
          'Date From': 'incur_date',
          # 'Discharge date': 'discharge_date',
          # 'File Date': 'submission_date',
          'Received Date': 'submission_date',
          'Date Notified': 'submission_date',
          'Claim Payment Date': 'pay_date',
          'Process Date': 'pay_date',
          'Date Paid': 'pay_date',
          # 'claim_status',
          'Remark1': 'claim_remark',
          # 'cert_true_copy',
          'Member ID': 'claimant',
          'Claimant No.': 'claimant',
          'Claimant Name': 'claimant',
          'Gender': 'gender',
          'Sex': 'gender', # M/F
          'Age': 'age',
          'Relationships': 'dep_type', # MEMBER/ SPOUSE/ CHILD
          'Dep Type': 'dep_type', # M/ S/ C
          'Plan': 'class',
          'BenPlan': 'class',
          # 'member_status',
          'Claim Type': 'benefit_type',
          'Product Name': 'benefit_type',
          'Claim Type 2': 'benefit',
          'Ben Desc': 'benefit',
          'Diagnosis': 'diagnosis',
          # 'chronic',
          'Billed Currency': 'currency',
          'Currency Symbol': 'currency', 
          'Submitted Claim Amount': 'incurred_amount',
          'Presented Amount': 'incurred_amount',
          'Amount Billed': 'incurred_amount',
          'Paid Claim': 'paid_amount',
          'Reimbursed Amount': 'paid_amount',
          'Amount Paid': 'paid_amount',
          'Provider Type': 'panel',
          'Network/Non-network': 'panel',
          'SubOffice': 'suboffice',
          # 'Entity': 'suboffice',
          # 'region',
      }


    date_cols = ['policy_start_date', 'policy_end_date', 'incur_date', 'discharge_date', 'submission_date', 'pay_date']

    if raw_claim_path.split('.')[-1] == 'xls':
      print('Please save this excel into xlsx format. *NOT Strict Open XML Spreadsheet')
      return


    if password == None:
      t_df = pd.read_excel(raw_claim_path, dtype=dtype_aia)
    else:
      unlocked_file = io.BytesIO()
      with open(raw_claim_path, "rb") as file:
        excel_file = msoffcrypto.OfficeFile(file)
        excel_file.load_key(password = password)
        excel_file.decrypt(unlocked_file)
        from openpyxl import load_workbook
        wb = load_workbook(filename = unlocked_file)
      t_df = pd.read_excel(unlocked_file, dtype=dtype_aia)

    t_df.rename(columns=aia_rename_col, inplace=True)
    t_df['insurer'] = 'AIA'

    for col in date_cols:
      if col in t_df.columns.tolist():
        print(t_df[col].values[0])
        try:
          t_df[col] = pd.to_datetime(t_df[col], format='ISO8601')
        except:
          try:
            t_df[col] = pd.to_datetime(t_df[col], format='%Y%m%d')
          except:
            try:
              t_df[col] = pd.to_datetime(t_df[col], format='%y-%b')
            except:
              try:
                t_df[col] = pd.to_datetime(t_df[col], format='%Y-%m-%d')
              except:
                t_df[col] = pd.to_datetime(t_df[col])
      else:
        t_df[col] = np.nan

    if policy_start_date != None:
      t_df['policy_start_date'] = pd.to_datetime(policy_start_date, format='%Y%m%d')

    if client_name != None:
      t_df['client_name'] = client_name

    __start_date = t_df['policy_start_date'].iloc[0]
    __policy_number = t_df.policy_number.values[0]
    t_df['policy_id'] = f'{__policy_number}_{__start_date:%Y%m}'


    t_df['claim_status'] = np.nan
    t_df['cert_true_copy'] = np.nan
    t_df['member_status'] = np.nan
    t_df['chronic'] = np.nan
    t_df['region'] = region

    for col in self.col_setup:
      if col not in t_df.columns.tolist():
        t_df[col] = np.nan
      elif t_df[col].dtype == 'object':
        t_df[col] = t_df[col].str.strip()


    aia_index = self.benefit_index[['gum_benefit', 'aia_benefit']]
    t_df.benefit = t_df.benefit.str.strip()
    t_df = pd.merge(left=t_df, right=aia_index, left_on='benefit', right_on='aia_benefit', how='left')
    t_df.benefit = t_df.gum_benefit


    t_df = t_df[self.col_setup]

    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('aso', case=False)] = 'ASO'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('hosp', case=False)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('clin', case=False)] = 'Clinic'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('dent', case=False)] = 'Dental'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('supp', case=False)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('mat', case=False)] = 'Maternity'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('opt', case=False)] = 'Optical'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('well', case=False)] = 'Wellness'

    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('inpatient', case=False)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('outpatient', case=False)] = 'Clinic'

    t_df.diagnosis.fillna('No diagnosis provided', inplace=True)
    if len(t_df.diagnosis.unique()) > 2:
      t_df['benefit_type'].loc[t_df['diagnosis'].str.contains('dental', case=False)] = 'Dental'
      t_df['benefit'].loc[t_df['diagnosis'].str.contains('dental', case=False)] = 'Dental'
    #bupa_b_type = {'C': 'Clinic', 'H': 'Hospital', 'D': 'Dental', 'G': 'Optical', 'T': 'Hospital', 'W': 'Hospital', 'M': 'Maternity'}
    aia_dep_type = {'M': 'EE', 'S':'SP', 'C': 'CH', 'E': 'EE'}
    t_df['dep_type'].replace(aia_dep_type, inplace=True)
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('mem', case=False)] = 'EE'
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('emp', case=False)] = 'EE'
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('spo', case=False)] = 'SP'
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('chi', case=False)] = 'CH'
    t_df.gender.loc[t_df.gender.str.contains('f', case=False)] = 'F'
    t_df.gender.loc[t_df.gender.str.contains('m', case=False)] = 'M'
    t_df.panel.replace({'Non-network': 'Non-Panel', 'Network': 'Panel'}, inplace=True)
    t_df.panel.fillna('Non-Panel', inplace=True)
    t_df.suboffice.fillna('00', inplace=True)


    return t_df
  
  def blue_cross_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      dtype_bluecross_raw = {
          'INPUT CLAIM TYPE': str,
          'POLICY NO.': str,
          'MEMBER': str,
          'DEPENDANT CODE': str,
          'LEVEL CODE': str,
          'BENEFIT NAME': str,
          'CLAIM DATE': str,
          'CLAIM AMOUNT': float,
          'PAID AMOUNT': float,
          'REJECT REASON': str,
      }
      bluecross_rename_col = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY NO.': 'policy_number',
        # 'insurer',
        # 'client_name',
        # 'policy_start_date',
        # 'policy_end_date',
        # 'claim_id',
        'CLAIM DATE': 'incur_date',
        # 'discharge_date',
        # 'submission_date',
        # 'pay_date',
        # 'claim_status',
        'REJECT REASON': 'claim_remark',
        # 'cert_true_copy',
        'MEMBER': 'claimant',
        # 'gender',
        # 'age',
        'DEPENDANT CODE': 'dep_type',
        #'class',
        # 'member_status',
        # 'benefit_type',
        'BENEFIT NAME': 'benefit',
        # 'diagnosis',
        # 'procedure',
        # 'hospital_name',
        # 'chronic',
        # 'currency',PAID AMOUNT
        'CLAIM AMOUNT': 'incurred_amount',
        'PAID AMOUNT': 'paid_amount',
        'INPUT CLAIM TYPE': 'panel',
        # 'suboffice',
        # 'region',

        'LEVEL CODE': 'level_code',
      }
      date_cols = ['incur_date']
    
    
    t_df = pd.read_excel(raw_claim_path, dtype=dtype_bluecross_raw)
    t_df.rename(columns=bluecross_rename_col, inplace=True)

    for date_col in date_cols:
      if date_col in t_df.columns.tolist():
        t_df[date_col] = pd.to_datetime(t_df[date_col], format='%Y%m%d')
      else:
        t_df[date_col] = np.nan

    if policy_start_date != None:
      t_df['policy_start_date'] = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      t_df['policy_start_date'] = t_df['incur_date'].sort_values().iloc[0]

    t_df['policy_end_date'] = t_df['policy_start_date'] + pd.DateOffset(years=1)
    t_df['insurer'] = 'Blue Cross'
    t_df['client_name'] = client_name
    t_df['policy_start_date'] = pd.to_datetime(t_df['policy_start_date'])
    _start_date = t_df['policy_start_date'].iloc[0]
    t_df['policy_id'] = f'{t_df.policy_number.values[0]}_{_start_date:%Y%m}'
    t_df['claim_status'] = np.nan
    t_df['claim_status'].loc[t_df['claim_remark'].isna() == False] = 'R'
    t_df['panel'].replace({'E': 'Panel', 'I': 'Non-Panel', 'O': 'Non-Panel'}, inplace=True)
    t_df['region'] = region
    t_df['class'] = t_df['level_code'].str.split(' ').str[-1].str[0]
    t_df['benefit_type'] = t_df['level_code'].str.split(' ').str[0]
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('HS', case=True)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('OP', case=True)] = 'Clinic'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('DENT', case=True)] = 'Dental'
    t_df['suboffice'] = "00"
    t_df['dep_type'].fillna('EE', inplace=True)
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('H|W', case=False)] = 'SP'
    t_df['dep_type'].loc[t_df['dep_type'].str.contains('C', case=False)] = 'CH'
    

    bluecross_benefit = self.benefit_index[['gum_benefit', 'blue_cross_benefit']]
    t_df = pd.merge(left=t_df, right=bluecross_benefit, left_on='benefit', right_on='blue_cross_benefit', how='left')
    t_df.benefit = t_df.gum_benefit
    if 'diagnosis' not in t_df.columns.tolist():
      t_df['diagnosis'] = 'No diagnosis provided'
    else:
      t_df['diagnosis'].fillna('No diagnosis provided', inplace=True)



    for col in self.col_setup:
      if col not in t_df.columns.tolist():
        t_df[col] = np.nan
    t_df = t_df[self.col_setup]

    return t_df
  
  def axa_raw_single(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      dtype_axa_raw = {
          'POLICY_HOLDER_NO': str,
          'CERT_NO': str,
          'PATIENT': str,
          'DEP_NO': str,
          'ROLE': str,
          'AGE': int,
          'GENDER': str,
          'CLAIM_NO': str,
          'ADMISSION/CONSULTATION_DATE': str,
          'DISCHARGE_DATE': str,
          'PAYMENT_DATE': str,
          'HOSPITAL': str,
          'DOCTOR': str,
          'DIAGNOSIS': str,
          'PROCEDURE': str,
          'STATUS': str,
          'NETWORK': str,
          'BENEFIT_CODE': str,
          'CURRENCY': str,
          'CLAIMS_AMOUNT(HKD)': float,
          'CLAIM_PAID_AMOUNT(HKD)': float,
          'CLASS': str,
          'AFFILIATED_CODE': str,
          'REMARK_1': str,
          'CLAIM_TYPE': str,
      }
      axa_rename_col = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        'POLICY_HOLDER_NO': 'policy_number',
        # 'insurer',
        # 'client_name',
        # 'policy_start_date',
        # 'policy_end_date',
        'CLAIM_NO': 'claim_id',
        'ADMISSION/CONSULTATION_DATE': 'incur_date',
        'DISCHARGE_DATE': 'discharge_date',
        # 'submission_date',
        'PAYMENT_DATE': 'pay_date',
        'STATUS': 'claim_status',
        'REMARK_1': 'claim_remark',
        # 'cert_true_copy',
        'PATIENT': 'claimant',
        'GENDER': 'gender',
        'AGE': 'age',
        'ROLE': 'dep_type',
        'CLASS': 'class',
        # 'member_status',
        'CLAIM_TYPE': 'benefit_type',
        'BENEFIT_CODE': 'benefit',
        'DIAGNOSIS': 'diagnosis',
        'PROCEDURE': 'procedure',
        'HOSPITAL': 'hospital_name',
        'DOCTOR': 'physician',
        # 'chronic',
        'CURRENCY': 'currency',
        'CLAIMS_AMOUNT(HKD)': 'incurred_amount',
        'CLAIM_PAID_AMOUNT(HKD)': 'paid_amount',
        'NETWORK': 'panel',
        'AFFILIATED_CODE': 'suboffice',
        # 'region',
      }
    date_cols = ['incur_date', 'discharge_date', 'pay_date']
    t_df = pd.read_excel(raw_claim_path, dtype=dtype_axa_raw)
    t_df.rename(columns=axa_rename_col, inplace=True)
    for col in date_cols:
      if col in t_df.columns.tolist():
        t_df[col] = pd.to_datetime(t_df[col])
      else:
        t_df[col] = np.nan
    if policy_start_date != None:
      t_df['policy_start_date'] = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      t_df['policy_start_date'] = t_df['incur_date'].sort_values().iloc[0]
    t_df['policy_end_date'] = t_df['policy_start_date'] + pd.DateOffset(years=1)
    t_df['insurer'] = 'AXA'
    t_df['client_name'] = client_name
    _start_date = t_df['policy_start_date'].iloc[0]
    t_df['policy_id'] = f'{t_df.policy_number.values[0]}_{_start_date:%Y%m}'
    t_df['dep_type'].replace({'EMPLOYEE': 'EE', 'SPOUSE': 'SP', 'CHILD': 'CH'}, inplace=True)
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('inp', case=False)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('out', case=False)] = 'Clinic'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('den', case=False)] = 'Dental'
    t_df['panel'].replace({'Network': 'Panel', 'Non-Network': 'Non-Panel'}, inplace=True)

    axa_benefit = self.benefit_index[['gum_benefit', 'axa_benefit_code']]
    t_df = pd.merge(left=t_df, right=axa_benefit, left_on='benefit', right_on='axa_benefit_code', how='left')
    t_df.benefit = t_df.gum_benefit
    t_df['region'] = region

    for col in self.col_setup:
      if col not in t_df.columns.tolist():
        t_df[col] = np.nan
    t_df = t_df[self.col_setup]

    return t_df


  def lfh_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      dtype_lfh_raw = {
          'policy_number': str,
          # 'Policy Holder': str,
          # 'Dependant': str,
          # 'Relationship
          'Claim no': str,
          'Cert no': str,
          'Benefit code': str,
          'Benefit Name': str,
          'Plan': str,
          'Claim date': str,
          'Claim amt': float,
          'Paid date': str,
          'Paid amt': float,
      }
      lfh_rename_col = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        #'Claim no': 'policy_number',
        #'insurer',
        #'client_name',
        #'policy_start_date',
        #'policy_end_date',
        'Claim no': 'claim_id',
        'Claim date': 'incur_date',
        # 'discharge_date',
        # 'submission_date',
        'Paid date': 'pay_date',
        #'claim_status',
        #'claim_remark',
        #'cert_true_copy',
        'Cert no': 'claimant',
        #'gender',
        #'age',
        #'dep_type',
        'Plan': 'class',
        #'member_status',
        'Benefit Name': 'benefit_type',
        #'benefit',
        #'diagnosis',
        #'procedure',
        #'hospital_name',
        #'chronic',
        #'currency',
        'Claim amt': 'incurred_amount',
        'Paid amt': 'paid_amount',
        # 'panel',
        # 'suboffice',
        # 'region',
      }
    date_cols = ['incur_date', 'pay_date']
    t_df = pd.read_excel(raw_claim_path, dtype=dtype_lfh_raw)
    t_df.rename(columns=lfh_rename_col, inplace=True)
    for col in date_cols:
      if col in t_df.columns.tolist():
        t_df[col] = pd.to_datetime(t_df[col])
      else:
        t_df[col] = np.nan
    if policy_start_date != None:
      t_df['policy_start_date'] = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      t_df['policy_start_date'] = t_df['incur_date'].sort_values().iloc[0]
    t_df['policy_end_date'] = t_df['policy_start_date'] + pd.DateOffset(years=1)
    t_df['insurer'] = 'LFH'
    t_df['client_name'] = client_name
    _start_date = t_df['policy_start_date'].iloc[0]
    t_df['policy_id'] = f'{t_df.policy_number.values[0]}_{_start_date:%Y%m}'
    ##############################################################################################################
    # Since LFH does not have the dep_type, we will assume that all the claims are EE
    # LFH does not have the benefit, we will assume that all the benefits are the same as the benefit_type
    ##############################################################################################################
    t_df['dep_type'] = 'EE'
    t_df['benefit'] = t_df['benefit_type']
    t_df['region'] = region
    t_df['suboffice'] = '00'
    if 'diagnosis' not in t_df.columns.tolist():
      t_df['diagnosis'] = 'No diagnosis provided'
    else:
      t_df['diagnosis'].fillna('No diagnosis provided', inplace=True)
    
    for col in self.col_setup:
      if col not in t_df.columns.tolist():
        t_df[col] = np.nan
    t_df = t_df[self.col_setup]

    return t_df

  def hsbc_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):
    if col_mapper == None:
      dtype_hsbc_raw = {
          "POLICY NO.": str,
          'Masked ID': str,
          'DEPENDANT CODE': str,
          'Next Anniversary Date': str,
          'Gender': str,
          'Medical Tier': str,
          'Termination Date': str,
          'Effective Date': str,
          'Claim Type': str,
          'BENEFIT DESCRIPTION': str,
          'CLAIM STATUS': str,
          'CLAIM INCURRED DATE': str,
          'CLAIM INCURRED AMOUNT': float,
          'CLAIM PROCESSED DATE': str,
          'CLAIM PAID AMOUNT': float,
          'Claim Pay': str,
          'Payment method': str,
          'Currency': str,
          'REMARK DESCRIPTION': str,
      }
      hsbc_rename_col = {
        "POLICY NO.": "policy_number",
        "Masked ID": "claimant",
        "DEPENDANT CODE": "dep_type",
        "Next Anniversary Date": "policy_start_date",
        "Gender": "gender",
        "Medical Tier": "class",
        # "Termination Date"	
        # "Effective Date" 
        "Claim Type": "benefit_type",
        "BENEFIT DESCRIPTION": "benefit",
        "CLAIM STATUS": "claim_status",	
        "CLAIM INCURRED DATE": "incur_date",	
        "CLAIM INCURRED AMOUNT": "incurred_amount",	
        "CLAIM PROCESSED DATE": "pay_date",	
        "CLAIM PAID AMOUNT": "paid_amount",
        # "Claim Pay": 	
        # "Payment method"	
        "Currency": "currency",
        "REMARK DESCRIPTION": "claim_remark",}
    # 'policy_id', # This is the policy_id for future
    date_cols = ['incur_date', 'pay_date']
    t_df = pd.read_excel(raw_claim_path, dtype=dtype_hsbc_raw)
    t_df.rename(columns=hsbc_rename_col, inplace=True)
    for col in date_cols:
      if col in t_df.columns.tolist():
        t_df[col] = pd.to_datetime(t_df[col])
      else:
        t_df[col] = np.nan
    if policy_start_date != None:
      t_df['policy_start_date'] = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      t_df['policy_start_date'] = t_df['incur_date'].sort_values().iloc[0]
    t_df['policy_end_date'] = t_df['policy_start_date'] + pd.DateOffset(years=1)
    t_df['insurer'] = 'HSBC'
    t_df['client_name'] = client_name
    _start_date = t_df['policy_start_date'].iloc[0]
    t_df['policy_id'] = f'{t_df.policy_number.values[0]}_{_start_date:%Y%m}'
    
    t_df['dep_type'].replace({"1": "EE", "2": "SP"}, inplace=True)
    t_df['dep_type'].loc[t_df['dep_type'].isin(["EE", "SP"]) == False] = "CH"
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('HOSP', case=False)] = 'Hospital'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('CLIN', case=False)] = 'Clinic'
    t_df['benefit_type'].loc[t_df['benefit_type'].str.contains('SMM', case=False)] = 'Hospital'
    hsbc_index = self.benefit_index[['gum_benefit', 'hsbc_benefit']]
    t_df = pd.merge(left=t_df, right=hsbc_index, left_on='benefit', right_on='hsbc_benefit', how='left')
    t_df.benefit = t_df.gum_benefit
    t_df['region'] = region
    t_df['suboffice'] = '00'
    if 'diagnosis' not in t_df.columns.tolist():
      t_df['diagnosis'] = 'No diagnosis provided'
    else:
      t_df['diagnosis'].fillna('No diagnosis provided', inplace=True)
    
    for col in self.col_setup:
      if col not in t_df.columns.tolist():
        t_df[col] = np.nan
    t_df = t_df[self.col_setup]
    return t_df



  def __consol_raw_claim(self, raw_claim_path):
    dtype_con_raw = {
      'policy_id': str,
      'policy_number': str,
      'insurer': str,
      'client_name': str,
      'policy_start_date': str,
      'policy_end_date': str,
      'claim_id': str,
      'incur_date': str,
      'discharge_date': str,
      'submission_date': str,
      'pay_date': str,
      'claim_status': str,
      'claim_remark': str,
      'cert_true_copy': str,
      'claimant': str,
      'gender': str,
      'age': str,
      'dep_type': str,
      'class': str,
      'member_status': str,
      'benefit_type': str,
      'benefit': str,
      'diagnosis': str,
      'chronic': str,
      'currency': str,
      'incurred_amount': float,
      'paid_amount': float,
      'panel': str,
      'suboffice': str,
      'region': str,
    }
    date_cols = ['policy_start_date', 'policy_end_date', 'incur_date', 'discharge_date', 'submission_date', 'pay_date']

    if raw_claim_path.split('.')[-1] == 'csv':
      t_df = pd.read_csv(raw_claim_path, dtype= dtype_con_raw)
    else:
      t_df = pd.read_excel(raw_claim_path, dtype= dtype_con_raw)

    for col in date_cols:
      if len(t_df[col].unique()) > 1:
        try:
          t_df[col].loc[~(t_df[col].isna())] = pd.to_datetime(t_df[col].loc[~(t_df[col].isna())], format='%m/%d/%y')
        except:
          t_df[col].loc[~(t_df[col].isna())] = pd.to_datetime(t_df[col].loc[~(t_df[col].isna())], format='%Y-%m-%d')

    return t_df


  def add_raw_data(self, raw_claim_path, insurer=None, password=None, policy_start_date=None, client_name=None, region='HK', col_mapper=None):

    if insurer == 'AXA':
      temp_df = self.__axa_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'AIA':
      temp_df = self.__aia_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'Bupa':
      temp_df = self.__bupa_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'Blue Cross':
      temp_df = self.blue_cross_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'AXA single':
      temp_df = self.axa_raw_single(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'LFH':
      temp_df = self.lfh_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    elif insurer == 'HSBC':
      temp_df = self.hsbc_raw_claim(raw_claim_path, password, policy_start_date, client_name, region, col_mapper)
    else:
      # print('Please make sure that the colums of the DataFrame is aligned with the standard format')
      temp_df = self.__consol_raw_claim(raw_claim_path)
    self.df = pd.concat([self.df, temp_df], axis=0, ignore_index=True)

    self.policy_id_list = self.df['policy_id'].unique().tolist()
    self.policy_list = self.df['policy_number'].unique().tolist()
    self.clinet_name_list = self.df['client_name'].unique().tolist()
    self.benefit_type_list = self.df['benefit_type'].unique().tolist()
    self.benefit_list = self.df['benefit'].unique().tolist()

  def bupa_shortfall_supplement(self, shortfall_processed_df):
    # shortfall_panel = shortfall_processed_df.loc[shortfall_processed_df['panel'] == 'Panel']
    shortfall_panel = shortfall_processed_df.loc[shortfall_processed_df['panel'] == 'Overall']
    for n00 in np.arange(len(shortfall_panel)):
      __policy_id = shortfall_panel['policy_id'].iloc[n00]
      __class = shortfall_panel['class'].iloc[n00]
      __benefit = shortfall_panel['benefit'].iloc[n00]
    
      __no_of_claims = shortfall_panel['no_of_claims'].iloc[n00] - \
        self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Non-Panel')].dropna().count() - \
        self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel')].dropna().count()
      
      if __no_of_claims > 0:
        __incurred_amount = shortfall_panel['incurred_amount'].iloc[n00] - \
          self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Non-Panel')].dropna().sum() - \
          self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel')].dropna().sum()
        
        __paid_amount = shortfall_panel['paid_amount'].iloc[n00] - \
          self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Non-Panel')].dropna().sum() -\
          self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel')].dropna().sum()

        t_incur_per_claim = __incurred_amount / __no_of_claims
        t_paid_per_claim = __paid_amount / __no_of_claims

        self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel') & (pd.isna(self.df['incurred_amount']) == True)] = t_incur_per_claim
        self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel') & (pd.isna(self.df['paid_amount']) == True)] = t_paid_per_claim
    return



  def preprocessing(self, policy_id=None, rejected_claim=True, aso=True, smm=True, diagnosis=False):
    if aso == True:
      self.df = self.df.loc[self.df.benefit_type != 'ASO']

    if rejected_claim == True:
      reject_claim_words = ['submit', 'resumit', 'submission', 'receipt', 'signature', 'photo', 'provide', 'form']
      self.df.claim_remark.fillna('no_remark', inplace=True)
      # Bupa claim remark = Reject Code so it must be rejected
      self.df.claim_status.loc[(self.df.insurer == 'Bupa') & (self.df.claim_remark != 'no_remark')] = 'R'
      self.df.claim_status.loc[self.df.claim_remark.str.contains('|'.join(reject_claim_words), case=False) & ((self.df.paid_amount == 0) | (self.df.paid_amount.isna()))] = 'R'
      self.df.claim_status.loc[(self.df.insurer == 'AXA') & (self.df.claim_status == 'R') & (self.df.paid_amount != 0)] = 'PR'
      self.df.claim_status.loc[(self.df.insurer == 'HSBC') & (self.df.claim_status.isin(["Pending", "Processing", "Rejected", "Withdrawn"]) == True)] = 'R'
      self.df = self.df.loc[(self.df.claim_status != 'R')]
      # self.df = self.df.loc[(self.df.claim_remark == 'no_remark')]
      

    if smm == True:
      self.df.benefit.fillna('no_benefit', inplace=True)
      self.df.incurred_amount.loc[self.df.benefit == 'Top-up/SMM'] = 0
      self.df.incurred_amount.loc[self.df.benefit.str.contains('secondary claim', case=False)] = self.df.paid_amount.loc[self.df.benefit.str.contains('secondary claim', case=False)]
      self.df.incurred_amount.loc[self.df.benefit.str.contains('daily cash benefit', case=False)] = self.df.paid_amount.loc[self.df.benefit.str.contains('daily cash benefit', case=False)]

    if diagnosis == True:
      self.df.diagnosis = self.df.diagnosis.str.lower()
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('acute upper respiratory infec|common cold', case=False)] = 'acute upper respiratory infection & common cold'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains(', unspec', case=False)] = self.df.diagnosis.loc[self.df.diagnosis.str.contains(', unspec', case=False)].replace(to_replace={', unspec': '', ', unspecified': ''})
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('chlamydia', case=False)] = 'viral warts'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('dermatitis|eczema', case=False)] = 'dermatitis & eczema'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('gastritis|gastroenteritis|intestinal infec', case=False)] = 'gastritis and gastroenteritis'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('benign neoplasm of colon|polyp of colon|anal and rectal polyp', case=False)] = 'benign neoplasm of colon & polyp'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('benign neoplasm of stomach|polyp of stomach and duodenum', case=False)] = 'benign neoplasm of stomach & polyp'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('influenza', case=False)] = 'influenza'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('lumbar sprain|sprain and strain of lumbar spine|backache|low back pain|sprain of unspec site of back', case=False)] = 'lumbago'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('pain in joint|pain in ankle and joints of foot|sprains and strains of ankle and foot', case=False)] = 'pain in joint and ankle'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('neck sprain|cervicalgia', case=False)] = 'cervicalgia (neck pain)'
      self.df.diagnosis.loc[self.df.diagnosis.str.contains('cataract', case=False)] = 'cataract'
      print('changed')

    return None

  def mcr_p20_policy(self, by=None):
    if by == None:
      __p20_policy_df_col = ['policy_number', 'year', 'incurred_amount', 'paid_amount', 'claimant']
      __p20_policy_group_col = ['policy_number', 'year']
    else: 

      __p20_policy_df_col = ['policy_number', 'year'] + by + ['incurred_amount', 'paid_amount', 'claimant']
      __p20_policy_group_col = ['policy_number', 'year'] + by
    
    self.mcr_df.policy_start_date = pd.to_datetime(self.mcr_df.policy_start_date)
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p20_policy_df = self.mcr_df[__p20_policy_df_col].groupby(by=__p20_policy_group_col, dropna=False).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claimant': 'nunique'}).rename(columns={'claimant': 'no_of_claimants'})
    p20_policy_df['usage_ratio'] = p20_policy_df['paid_amount'] / p20_policy_df['incurred_amount']
    p20_policy_df = p20_policy_df.unstack().stack(dropna=False)
    self.p20_policy = p20_policy_df
    return p20_policy_df

  def mcr_p20_benefit(self, by=None, benefit_type_order=['Hospital', 'Clinic', 'Dental', 'Optical', 'Maternity', 'Total']):
    if by == None:
      __p20_benefit_df_col = ['policy_number', 'year', 'benefit_type', 'incurred_amount', 'paid_amount', 'claimant']
      __p20_benefit_group_col = ['policy_number', 'year', 'benefit_type']
      __p20_benefit_claims_col = ['policy_number', 'year', 'benefit_type', 'incur_date']
    else: 

      __p20_benefit_df_col = ['policy_number', 'year'] + by + ['benefit_type', 'incurred_amount', 'paid_amount', 'claimant']
      __p20_benefit_group_col = ['policy_number', 'year'] + by + ['benefit_type']
      __p20_benefit_claims_col = ['policy_number', 'year'] + by + ['benefit_type', 'incur_date']
    

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p20_benefit_df = self.mcr_df[__p20_benefit_df_col].groupby(by=__p20_benefit_group_col, dropna=False).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claimant': 'nunique'}).rename(columns={'claimant': 'no_of_claimants'})
    for __policy_number in p20_benefit_df.index.get_level_values(0).unique():
      for __year in p20_benefit_df.index.get_level_values(1).unique():
          if by == None:
            p20_benefit_df.loc[__policy_number, __year, 'Total'] = p20_benefit_df.loc[__policy_number, __year, :].sum()
          else:
            for __l3 in p20_benefit_df.index.get_level_values(2).unique():
              p20_benefit_df.loc[__policy_number, __year, __l3, 'Total'] = p20_benefit_df.loc[__policy_number, __year, __l3, :].sum()
          # print(p20_benefit_df.loc[__policy_number, :].loc[__year, :].sum())

    p20_no_claims = self.mcr_df[__p20_benefit_claims_col].groupby(by=__p20_benefit_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p20_ip_no_claims = self.mcr_df[__p20_benefit_claims_col].loc[self.mcr_df['benefit'].str.contains('Day Centre|Surgeon', case=False) == True].groupby(by=__p20_benefit_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p20_no_claims['no_of_claims'].loc[p20_no_claims.index.get_level_values(len(__p20_benefit_group_col)-1) == 'Hospital'] = p20_ip_no_claims['no_of_claims']

    p20_benefit_df['usage_ratio'] = p20_benefit_df['paid_amount'] / p20_benefit_df['incurred_amount']
    p20_benefit_df['no_of_claims'] = p20_no_claims['no_of_claims']
    p20_benefit_df['incurred_per_claim'] = p20_benefit_df['incurred_amount'] / p20_benefit_df['no_of_claims']
    p20_benefit_df['paid_per_claim'] = p20_benefit_df['paid_amount'] / p20_benefit_df['no_of_claims']
    p20_benefit_df = p20_benefit_df.unstack().stack(dropna=False)
    p20_benefit_df = p20_benefit_df.reindex(benefit_type_order, level='benefit_type')

    p20_benefit_df = p20_benefit_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim', 'no_of_claimants']]

    self.p20_benefit = p20_benefit_df
    self.p20 = p20_benefit_df
    return p20_benefit_df

  def mcr_p20_panel(self, by=None):
    if by == None:
      __p20_panel_df_col = ['policy_number', 'year', 'panel', 'incurred_amount', 'paid_amount']
      __p20_panel_group_col = ['policy_number', 'year', 'panel']
    else: 
      __p20_panel_df_col = ['policy_number', 'year'] + by + ['panel', 'incurred_amount', 'paid_amount']
      __p20_panel_group_col = ['policy_number', 'year'] + by + ['panel']
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p20_panel_df = self.mcr_df[__p20_panel_df_col].groupby(by=__p20_panel_group_col, dropna=False).sum()
    p20_panel_df['usage_ratio'] = p20_panel_df['paid_amount'] / p20_panel_df['incurred_amount']
    p20_panel_df = p20_panel_df.unstack().stack(dropna=False)
    self.p20_panel = p20_panel_df
    return p20_panel_df
  
  def mcr_p20_panel_clin(self, by=None):
    if by == None:
      __p20_panel_df_col = ['policy_number', 'year', 'panel', 'incurred_amount', 'paid_amount', 'claim_id']
      __p20_panel_group_col = ['policy_number', 'year', 'panel']
    else: 
      __p20_panel_df_col = ['policy_number', 'year'] + by + ['panel', 'incurred_amount', 'paid_amount', 'claim_id']
      __p20_panel_group_col = ['policy_number', 'year'] + by + ['panel']
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p20_panel_clin_df = self.mcr_df.loc[self.mcr_df['benefit_type'] == 'Clinic'][__p20_panel_df_col].groupby(by=__p20_panel_group_col, dropna=False).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'count'}).rename(columns={'claim_id': 'no_of_claims'})
    p20_panel_clin_df['usage_ratio'] = p20_panel_clin_df['paid_amount'] / p20_panel_clin_df['incurred_amount']
    p20_panel_clin_df = p20_panel_clin_df.unstack().stack(dropna=False)
    self.p20_panel_clin = p20_panel_clin_df
    return p20_panel_clin_df

  def mcr_p21_class(self, by=None):
    if by == None:
      __p21_df_col = ['policy_number', 'year', 'class', 'incurred_amount', 'paid_amount', 'claimant']
      __p21_group_col = ['policy_number', 'year', 'class']
    else: 
      __p21_df_col = ['policy_number', 'year'] + by + ['class', 'incurred_amount', 'paid_amount', 'claimant']
      __p21_group_col = ['policy_number', 'year'] + by + ['class']

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p21_class_df = self.mcr_df[__p21_df_col].groupby(by=__p21_group_col, dropna=False).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claimant': 'nunique'}).rename(columns={'claimant': 'no_of_claimants'})
    p21_class_df['usage_ratio'] = p21_class_df['paid_amount'] / p21_class_df['incurred_amount']
    p21_class_df = p21_class_df.unstack().stack(dropna=False)
    self.p21_class = p21_class_df
    self.p21 = p21_class_df
    return p21_class_df

  def mcr_p22_class_benefit(self, by=None, benefit_type_order=None):
    if by == None:
      __p22_df_col = ['policy_number', 'year', 'class', 'benefit_type', 'incurred_amount', 'paid_amount']
      __p22_group_col = ['policy_number', 'year', 'class', 'benefit_type']
    else: 
      __p22_df_col = ['policy_number', 'year'] + by + ['class', 'benefit_type', 'incurred_amount', 'paid_amount']
      __p22_group_col = ['policy_number', 'year'] + by + ['class', 'benefit_type']

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p22_class_benefit_df = self.mcr_df[__p22_df_col].groupby(by=__p22_group_col, dropna=False).sum()
    p22_class_benefit_df['usage_ratio'] = p22_class_benefit_df['paid_amount'] / p22_class_benefit_df['incurred_amount']
    p22_class_benefit_df = p22_class_benefit_df.unstack().stack(dropna=False)
    p22_class_benefit_df = p22_class_benefit_df.reindex(benefit_type_order, level='benefit_type')
    self.p22_class_benefit = p22_class_benefit_df
    self.p22 = p22_class_benefit_df
    return p22_class_benefit_df

  def mcr_p23_ip_benefit(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY') | (self.benefit_index.gum_benefit_type == 'SUPPLEMENTARY MAJOR MEDICAL')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      __p23_df_col = ['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']
      __p23_group_col = ['policy_number', 'year', 'benefit']
      __p23_claims_col = ['policy_number', 'year', 'benefit', 'incur_date']
    else: 
      __p23_df_col = ['policy_number', 'year'] + by + ['benefit', 'incurred_amount', 'paid_amount']
      __p23_group_col = ['policy_number', 'year'] + by + ['benefit']
      __p23_claims_col = ['policy_number', 'year'] + by + ['benefit', 'incur_date']

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p23_ip_benefit_df = self.mcr_df[__p23_df_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p23_group_col, dropna=False).sum()
    p23_ip_benefit_df['usage_ratio'] = p23_ip_benefit_df['paid_amount'] / p23_ip_benefit_df['incurred_amount']
    p23_ip_no_claims = self.mcr_df[__p23_claims_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p23_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p23_ip_benefit_df['no_of_claims'] = p23_ip_no_claims['no_of_claims']
    p23_ip_benefit_df = p23_ip_benefit_df.unstack().stack(dropna=False)
    p23_ip_benefit_df = p23_ip_benefit_df.reindex(self.ip_order, level='benefit')
    self.p23_ip_benefit = p23_ip_benefit_df
    self.p23 = p23_ip_benefit_df
    return p23_ip_benefit_df

  def mcr_p23a_class_ip_benefit(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY') | (self.benefit_index.gum_benefit_type == 'SUPPLEMENTARY MAJOR MEDICAL')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      __p23a_df_col = ['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount']
      __p23a_group_col = ['policy_number', 'year', 'class', 'benefit']
      __p23a_claims_col = ['policy_number', 'year', 'class', 'benefit', 'incur_date']
    else: 
      __p23a_df_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incurred_amount', 'paid_amount']
      __p23a_group_col = ['policy_number', 'year'] + by + ['class', 'benefit']
      __p23a_claims_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incur_date']
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p23_ip_class_benefit_df = self.mcr_df[__p23a_df_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p23a_group_col, dropna=False).sum()
    p23_ip_class_benefit_df['usage_ratio'] = p23_ip_class_benefit_df['paid_amount'] / p23_ip_class_benefit_df['incurred_amount']
    p23_ip_class_no_claims = self.mcr_df[__p23a_claims_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p23a_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p23_ip_class_benefit_df['no_of_claims'] = p23_ip_class_no_claims['no_of_claims']
    p23_ip_class_benefit_df = p23_ip_class_benefit_df.unstack().stack(dropna=False)
    p23_ip_class_benefit_df = p23_ip_class_benefit_df.reindex(self.ip_order, level='benefit')
    self.p23a_ip_class_benefit = p23_ip_class_benefit_df
    self.p23a = p23_ip_class_benefit_df
    return p23_ip_class_benefit_df

  def mcr_p24_op_benefit(self, by=None):
    if by == None:
      __p24_df_col = ['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']
      __p24_group_col = ['policy_number', 'year', 'benefit']
      __p24_claims_col = ['policy_number', 'year', 'benefit', 'incur_date']
      __p24_claimants_col = ['policy_number', 'year', 'benefit', 'claimant']
      __p24_sort_col = ['policy_number', 'year', 'paid_amount']
      __p24_sort_order = [True, True, False]
    else: 
      __p24_df_col = ['policy_number', 'year'] + by + ['benefit', 'incurred_amount', 'paid_amount']
      __p24_group_col = ['policy_number', 'year'] + by + ['benefit']
      __p24_claims_col = ['policy_number', 'year'] + by + ['benefit', 'incur_date']
      __p24_claimants_col = ['policy_number', 'year'] + by + ['benefit', 'claimant']
      __p24_sort_col = ['policy_number', 'year'] + by + ['paid_amount']
      __p24_sort_order = [True, True] + len(by) * [True] + [False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p24_op_benefit_df = self.mcr_df[__p24_df_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p24_group_col, dropna=False).sum()
    p24_op_benefit_df['usage_ratio'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['incurred_amount']
    p24_op_no_claims = self.mcr_df[__p24_claims_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p24_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p24_op_claimant = self.mcr_df[__p24_claimants_col].loc[(self.mcr_df['benefit_type'] == 'Clinic')].drop_duplicates(subset=['policy_number', 'year', 'benefit', 'claimant'], keep='first').groupby(by=__p24_group_col).count().rename(columns={'claimant': 'no_of_claimants'})
    p24_op_benefit_df['no_of_claims'] = p24_op_no_claims['no_of_claims']
    p24_op_benefit_df['incurred_per_claim'] = p24_op_benefit_df['incurred_amount'] / p24_op_benefit_df['no_of_claims']
    p24_op_benefit_df['paid_per_claim'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['no_of_claims']
    p24_op_benefit_df['no_of_claimants'] = p24_op_claimant['no_of_claimants']
    # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
    p24_op_benefit_df = p24_op_benefit_df.unstack().stack(dropna=False)
    if len(p24_op_benefit_df) > 0:
      p24_op_benefit_df.sort_values(by=__p24_sort_col, ascending=__p24_sort_order, inplace=True)
    self.p24_op_benefit = p24_op_benefit_df
    self.p24 = p24_op_benefit_df
    return p24_op_benefit_df

  def mcr_p24a_op_class_benefit(self, by=None):

    if by == None:
      __p24a_df_col = ['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount']
      __p24a_group_col = ['policy_number', 'year', 'class', 'benefit']
      __p24a_claims_col = ['policy_number', 'year', 'class', 'benefit', 'incur_date']
      __p24a_sort_col = ['policy_number', 'year', 'class', 'paid_amount']
      __p24a_sort_order = [True, True, True, False]
    else: 
      __p24a_df_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incurred_amount', 'paid_amount']
      __p24a_group_col = ['policy_number', 'year'] + by + ['class', 'benefit']
      __p24a_claims_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incur_date']
      __p24a_sort_col = ['policy_number', 'year'] + by + ['class', 'paid_amount']
      __p24a_sort_order = [True, True] + len(by) * [True] + [True, False]

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p24_op_class_benefit_df = self.mcr_df[__p24a_df_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p24a_group_col, dropna=False).sum()
    p24_op_class_benefit_df['usage_ratio'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['incurred_amount']
    p24_op_class_no_claims = self.mcr_df[__p24a_claims_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p24a_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p24_op_class_benefit_df['no_of_claims'] = p24_op_class_no_claims['no_of_claims']
    p24_op_class_benefit_df['incurred_per_claim'] = p24_op_class_benefit_df['incurred_amount'] / p24_op_class_benefit_df['no_of_claims']
    p24_op_class_benefit_df['paid_per_claim'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['no_of_claims']
    # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
    p24_op_class_benefit_df = p24_op_class_benefit_df.unstack().stack(dropna=False)
    if len(p24_op_class_benefit_df) > 0:
      p24_op_class_benefit_df.sort_values(by=__p24a_sort_col, ascending=__p24a_sort_order, inplace=True)
    self.p24a_op_class_benefit = p24_op_class_benefit_df
    self.p24a = p24_op_class_benefit_df
    return p24_op_class_benefit_df

  def mcr_p24_dent_benefit(self, by=None):
    if by == None:
      __p24d_df_col = ['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']
      __p24d_group_col = ['policy_number', 'year', 'benefit']
      __p24d_claims_col = ['policy_number', 'year', 'benefit', 'incur_date']
      __p24d_sort_col = ['policy_number', 'year', 'paid_amount']
      __p24d_sort_order = [True, True, False]
    else: 
      __p24d_df_col = ['policy_number', 'year'] + by + ['benefit', 'incurred_amount', 'paid_amount']
      __p24d_group_col = ['policy_number', 'year'] + by + ['benefit']
      __p24d_claims_col = ['policy_number', 'year'] + by + ['benefit', 'incur_date']
      __p24d_sort_col = ['policy_number', 'year'] + by + ['paid_amount']
      __p24d_sort_order = [True, True] + len(by) * [True] + [False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p24_dent_benefit_df = self.mcr_df[__p24d_df_col].loc[self.mcr_df['benefit_type'] == 'Dental'].groupby(by=__p24d_group_col, dropna=False).sum()
    p24_dent_benefit_df['usage_ratio'] = p24_dent_benefit_df['paid_amount'] / p24_dent_benefit_df['incurred_amount']
    p24_dent_no_claims = self.mcr_df[__p24d_claims_col].loc[self.mcr_df['benefit_type'] == 'Dental'].groupby(by=__p24d_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p24_dent_benefit_df['no_of_claims'] = p24_dent_no_claims['no_of_claims']
    p24_dent_benefit_df['incurred_per_claim'] = p24_dent_benefit_df['incurred_amount'] / p24_dent_benefit_df['no_of_claims']
    p24_dent_benefit_df['paid_per_claim'] = p24_dent_benefit_df['paid_amount'] / p24_dent_benefit_df['no_of_claims']
    # p24_dent_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
    # print(p24_dent_benefit_df)
    p24_dent_benefit_df = p24_dent_benefit_df.unstack().stack(dropna=False)
    if len(p24_dent_benefit_df) > 0:
      p24_dent_benefit_df.sort_values(by=__p24d_sort_col, ascending=__p24d_sort_order, inplace=True)
    self.p24_dent_benefit = p24_dent_benefit_df
    return p24_dent_benefit_df
  
  def mcr_p24_wellness_benefit(self, by=None):
    if by == None:
      __p24w_df_col = ['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']
      __p24w_group_col = ['policy_number', 'year', 'benefit']
      __p24w_claims_col = ['policy_number', 'year', 'benefit', 'incur_date']
      __p24w_sort_col = ['policy_number', 'year', 'paid_amount']
      __p24w_sort_order = [True, True, False]
    else: 
      __p24w_df_col = ['policy_number', 'year'] + by + ['benefit', 'incurred_amount', 'paid_amount']
      __p24w_group_col = ['policy_number', 'year'] + by + ['benefit']
      __p24w_claims_col = ['policy_number', 'year'] + by + ['benefit', 'incur_date']
      __p24w_sort_col = ['policy_number', 'year'] + by + ['paid_amount']
      __p24w_sort_order = [True, True] + len(by) * [True] + [False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p24_wellness_benefit_df = self.mcr_df[__p24w_df_col].loc[(self.mcr_df['benefit_type'] == 'Dental') | (self.mcr_df['benefit_type'] == 'Optical') | (self.mcr_df['benefit'].str.contains('vaccin|check|combined', case=False))].groupby(by=__p24w_group_col, dropna=False).sum()
    p24_wellness_benefit_df['usage_ratio'] = p24_wellness_benefit_df['paid_amount'] / p24_wellness_benefit_df['incurred_amount']
    p24_wellness_no_claims = self.mcr_df[__p24w_claims_col].loc[(self.mcr_df['benefit_type'] == 'Dental') | (self.mcr_df['benefit_type'] == 'Optical') | (self.mcr_df['benefit'].str.contains('vaccin|check|combined', case=False))].groupby(by=__p24w_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p24_wellness_benefit_df['no_of_claims'] = p24_wellness_no_claims['no_of_claims']
    p24_wellness_benefit_df['incurred_per_claim'] = p24_wellness_benefit_df['incurred_amount'] / p24_wellness_benefit_df['no_of_claims']
    p24_wellness_benefit_df['paid_per_claim'] = p24_wellness_benefit_df['paid_amount'] / p24_wellness_benefit_df['no_of_claims']
    # p24_dent_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
    # print(p24_dent_benefit_df)
    p24_wellness_benefit_df = p24_wellness_benefit_df.unstack().stack(dropna=False)
    if len(p24_wellness_benefit_df) > 0:
      p24_wellness_benefit_df.sort_values(by=__p24w_sort_col, ascending=__p24w_sort_order, inplace=True)
    self.p24_wellness_benefit = p24_wellness_benefit_df
    return p24_wellness_benefit_df
  
  def mcr_p24_class_wellness_benefit(self, by=None):
    if by == None:
      __p24wc_df_col = ['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount']
      __p24wc_group_col = ['policy_number', 'year', 'class', 'benefit']
      __p24wc_claims_col = ['policy_number', 'year', 'class', 'benefit', 'incur_date']
      __p24wc_sort_col = ['policy_number', 'year', 'class', 'paid_amount']
      __p24wc_sort_order = [True, True, True, False]
    else: 
      __p24wc_df_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incurred_amount', 'paid_amount']
      __p24wc_group_col = ['policy_number', 'year'] + by + ['class', 'benefit']
      __p24wc_claims_col = ['policy_number', 'year'] + by + ['class', 'benefit', 'incur_date']
      __p24wc_sort_col = ['policy_number', 'year'] + by + ['class', 'paid_amount']
      __p24wc_sort_order = [True, True] + len(by) * [True] + [True, False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p24_class_wellness_benefit_df = self.mcr_df[__p24wc_df_col].loc[(self.mcr_df['benefit_type'] == 'Dental') | (self.mcr_df['benefit_type'] == 'Optical') | (self.mcr_df['benefit'].str.contains('vaccin|check|combined', case=False))].groupby(by=__p24wc_group_col, dropna=False).sum()
    p24_class_wellness_benefit_df['usage_ratio'] = p24_class_wellness_benefit_df['paid_amount'] / p24_class_wellness_benefit_df['incurred_amount']
    p24_class_wellness_no_claims = self.mcr_df[__p24wc_claims_col].loc[(self.mcr_df['benefit_type'] == 'Dental') | (self.mcr_df['benefit_type'] == 'Optical') | (self.mcr_df['benefit'].str.contains('vaccin|check|combined', case=False))].groupby(by=__p24wc_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p24_class_wellness_benefit_df['no_of_claims'] = p24_class_wellness_no_claims['no_of_claims']
    p24_class_wellness_benefit_df['incurred_per_claim'] = p24_class_wellness_benefit_df['incurred_amount'] / p24_class_wellness_benefit_df['no_of_claims']
    p24_class_wellness_benefit_df['paid_per_claim'] = p24_class_wellness_benefit_df['paid_amount'] / p24_class_wellness_benefit_df['no_of_claims']
    # p24_dent_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
    # print(p24_dent_benefit_df)
    p24_class_wellness_benefit_df = p24_class_wellness_benefit_df.unstack().stack(dropna=False)
    if len(p24_class_wellness_benefit_df) > 0:
      p24_class_wellness_benefit_df.sort_values(by=__p24wc_sort_col, ascending=__p24wc_sort_order, inplace=True)
    self.p24_class_wellness_benefit = p24_class_wellness_benefit_df
    return p24_class_wellness_benefit_df

  def mcr_p25_class_panel_benefit(self, by=None, benefit_type_order=None):
    if by == None:
      __p25_df_col = ['policy_number', 'year', 'class', 'panel', 'benefit_type', 'incurred_amount', 'paid_amount']
      __p25_group_col = ['policy_number', 'year', 'class', 'panel', 'benefit_type']
    else: 
      __p25_df_col = ['policy_number', 'year'] + by + ['class', 'panel', 'benefit_type', 'incurred_amount', 'paid_amount']
      __p25_group_col = ['policy_number', 'year'] + by + ['class', 'panel', 'benefit_type']

    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p25_class_panel_benefit_df = self.mcr_df[__p25_df_col].groupby(by=__p25_group_col, dropna=False).sum()
    p25_class_panel_benefit_df['usage_ratio'] = p25_class_panel_benefit_df['paid_amount'] / p25_class_panel_benefit_df['incurred_amount']
    p25_class_panel_benefit_df = p25_class_panel_benefit_df.unstack().stack(dropna=False)
    p25_class_panel_benefit_df = p25_class_panel_benefit_df.reindex(benefit_type_order, level='benefit_type')
    self.p25_class_panel_benefit = p25_class_panel_benefit_df
    self.p25 = p25_class_panel_benefit_df
    return p25_class_panel_benefit_df

  def mcr_p26_op_panel(self, by=None):
    if by == None:
      __p26_df_col = ['policy_number', 'year', 'panel', 'benefit', 'incurred_amount', 'paid_amount', 'incur_date', 'claimant']
      __p26_group_col = ['policy_number', 'year', 'panel', 'benefit']
      #__p26_claims_col = ['policy_number', 'year', 'panel', 'benefit', 'incur_date']
      __p26_sort_col = ['policy_number', 'year', 'panel', 'paid_amount']
      __p26_sort_order = [True, True, True, False]
    else: 
      __p26_df_col = ['policy_number', 'year'] + by + ['panel', 'benefit', 'incurred_amount', 'paid_amount', 'incur_date', 'claimant']
      __p26_group_col = ['policy_number', 'year'] + by + ['panel', 'benefit']
      #__p26_claims_col = ['policy_number', 'year'] + by + ['panel', 'benefit', 'incur_date']
      __p26_sort_col = ['policy_number', 'year'] + by + ['panel', 'paid_amount']
      __p26_sort_order = [True, True] + len(by) * [True] + [True, False]
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p26_op_panel_df = self.mcr_df[__p26_df_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p26_group_col, dropna=False).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'incur_date': 'count', 'claimant': 'nunique'}).rename(columns={'incur_date': 'no_of_claims', 'claimant': 'no_of_claimants'})
    p26_op_panel_df['usage_ratio'] = p26_op_panel_df['paid_amount'] / p26_op_panel_df['incurred_amount']
    #p26_op_no_claims = self.mcr_df[__p26_claims_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p26_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    #p26_op_panel_df['no_of_claims'] = p26_op_no_claims['no_of_claims']
    p26_op_panel_df['incurred_per_claim'] = p26_op_panel_df['incurred_amount'] / p26_op_panel_df['no_of_claims']
    p26_op_panel_df['paid_per_claim'] = p26_op_panel_df['paid_amount'] / p26_op_panel_df['no_of_claims']
    p26_op_panel_df = p26_op_panel_df.unstack().stack(dropna=False)
    p26_op_panel_df = p26_op_panel_df[['incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim', 'no_of_claimants']]
    if len(p26_op_panel_df.index) > 0:
      p26_op_panel_df.sort_values(by=__p26_sort_col, ascending=__p26_sort_order, inplace=True)
    self.p26_op_panel = p26_op_panel_df
    self.p26 = p26_op_panel_df
    return p26_op_panel_df
  
  def mcr_p26_op_class_panel(self, by=None):
    if by == None:
      __p26_df_col = ['policy_number', 'year', 'class', 'panel', 'benefit', 'incurred_amount', 'paid_amount']
      __p26_group_col = ['policy_number', 'year', 'class', 'panel', 'benefit']
      __p26_claims_col = ['policy_number', 'year', 'class', 'panel', 'benefit', 'incur_date']
      __p26_sort_col = ['policy_number', 'year', 'class', 'panel', 'paid_amount']
      __p26_sort_order = [True, True, True, True, False]
    else: 
      __p26_df_col = ['policy_number', 'year', 'class'] + by + ['panel', 'benefit', 'incurred_amount', 'paid_amount']
      __p26_group_col = ['policy_number', 'year', 'class'] + by + ['panel', 'benefit']
      __p26_claims_col = ['policy_number', 'year', 'class'] + by + ['panel', 'benefit', 'incur_date']
      __p26_sort_col = ['policy_number', 'year', 'class'] + by + ['panel', 'paid_amount']
      __p26_sort_order = [True, True, True] + len(by) * [True] + [True, False]
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p26_op_class_panel_df = self.mcr_df[__p26_df_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p26_group_col, dropna=False).sum()
    p26_op_class_panel_df['usage_ratio'] = p26_op_class_panel_df['paid_amount'] / p26_op_class_panel_df['incurred_amount']
    p26_op_class_no_claims = self.mcr_df[__p26_claims_col].loc[self.mcr_df['benefit_type'] == 'Clinic'].groupby(by=__p26_group_col, dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
    p26_op_class_panel_df['no_of_claims'] = p26_op_class_no_claims['no_of_claims']
    p26_op_class_panel_df['incurred_per_claim'] = p26_op_class_panel_df['incurred_amount'] / p26_op_class_panel_df['no_of_claims']
    p26_op_class_panel_df['paid_per_claim'] = p26_op_class_panel_df['paid_amount'] / p26_op_class_panel_df['no_of_claims']
    p26_op_class_panel_df = p26_op_class_panel_df.unstack().stack(dropna=False)
    if len(p26_op_class_panel_df.index) > 0:
      p26_op_class_panel_df.sort_values(by=__p26_sort_col, ascending=__p26_sort_order, inplace=True)
    self.p26_op_class_panel = p26_op_class_panel_df

    return p26_op_class_panel_df
  
  def mcr_p26_ip_panel(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY') | (self.benefit_index.gum_benefit_type == 'SUPPLEMENTARY MAJOR MEDICAL')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      __p26_df_col = ['policy_number', 'year', 'panel', 'benefit', 'incurred_amount', 'paid_amount']
      __p26_group_col = ['policy_number', 'year', 'panel', 'benefit']
      __p26_claims_col = ['policy_number', 'year', 'panel', 'benefit', 'incur_date', 'claimant']
    else: 
      __p26_df_col = ['policy_number', 'year'] + by + ['panel', 'benefit', 'incurred_amount', 'paid_amount']
      __p26_group_col = ['policy_number', 'year'] + by + ['panel', 'benefit']
      __p26_claims_col = ['policy_number', 'year'] + by + ['panel', 'benefit', 'incur_date', 'claimant']
    
    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p26_ip_panel_df = self.mcr_df[__p26_df_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p26_group_col, dropna=False).sum()
    p26_ip_panel_df['usage_ratio'] = p26_ip_panel_df['paid_amount'] / p26_ip_panel_df['incurred_amount']
    p26_ip_no_claims = self.mcr_df[__p26_claims_col].loc[self.mcr_df['benefit_type'] == 'Hospital'].groupby(by=__p26_group_col, dropna=False).agg({'incur_date': 'count', 'claimant':'nunique'}).rename(columns={'incur_date': 'no_of_claims', 'claimant': 'no_of_claimants'})
    p26_ip_panel_df['no_of_claims'] = p26_ip_no_claims['no_of_claims']
    p26_ip_panel_df['no_of_claimants'] = p26_ip_no_claims['no_of_claimants']
    p26_ip_panel_df['incurred_per_claim'] = p26_ip_panel_df['incurred_amount'] / p26_ip_panel_df['no_of_claims']
    p26_ip_panel_df['paid_per_claim'] = p26_ip_panel_df['paid_amount'] / p26_ip_panel_df['no_of_claims']
    p26_ip_panel_df = p26_ip_panel_df.unstack().stack(dropna=False)
    p26_ip_panel_df = p26_ip_panel_df.reindex(self.ip_order, level='benefit')
    self.p26_ip_panel = p26_ip_panel_df
    return p26_ip_panel_df
  
  def mcr_p18a_top_diag_ip(self, by=None):
    if by == None or 'diagnosis' in by:
      __p18_df_col = ['policy_number', 'year', 'diagnosis', 'incurred_amount', 'paid_amount']
      __p18_group_col = ['policy_number', 'year', 'diagnosis']
      __p18_claimants_col = ['policy_number', 'year', 'diagnosis', 'claimant']
      __p18_claims_col = ['policy_number', 'year', 'diagnosis', 'claim_id']
      __p18_sort_col = ['policy_number', 'year', 'paid_amount']
      __p18_sort_order = [True, True, False]
    else: 
      __p18_df_col = ['policy_number', 'year'] + by + ['diagnosis', 'incurred_amount', 'paid_amount']
      __p18_group_col = ['policy_number', 'year'] + by + ['diagnosis']
      __p18_claimants_col = ['policy_number', 'year'] + by + ['diagnosis', 'claimant']
      __p18_claims_col = ['policy_number', 'year'] + by + ['diagnosis', 'claim_id']
      __p18_sort_col = ['policy_number', 'year'] + by + ['paid_amount']
      __p18_sort_order = [True, True] + len(by) * [True] + [False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p18a_df = self.mcr_df[__p18_df_col].loc[(self.mcr_df['benefit_type'] == 'Hospital')].groupby(by=__p18_group_col).sum()
    p18a_df_claimant = self.mcr_df[__p18_claimants_col].loc[(self.mcr_df['benefit_type'] == 'Hospital')].drop_duplicates(subset=['policy_number', 'year', 'diagnosis', 'claimant'], keep='first').groupby(by=__p18_group_col).count().rename(columns={'claimant': 'no_of_claimants'})
    p18a_df['no_of_claimants'] = p18a_df_claimant['no_of_claimants']
    p18a_df_claims = self.mcr_df[__p18_claims_col].loc[(self.mcr_df['benefit_type'] == 'Hospital')].drop_duplicates(subset=['policy_number', 'year', 'diagnosis', 'claim_id']).groupby(by=__p18_group_col).count().rename(columns={'claim_id': 'no_of_claims'})
    p18a_df['no_of_claims'] = p18a_df_claims['no_of_claims']
    p18a_df = p18a_df[['no_of_claimants', 'no_of_claims', 'incurred_amount', 'paid_amount']]
    # p18a_df = p18a_df.unstack().stack(dropna=False)
    if len(p18a_df) > 0:
      p18a_df.sort_values(by=__p18_sort_col, ascending=__p18_sort_order, inplace=True)
    self.p18a = p18a_df
    return p18a_df
  
  def mcr_p18b_top_diag_op(self, by=None):
    if by == None or 'diagnosis' in by:
      __p18_df_col = ['policy_number', 'year', 'diagnosis', 'incurred_amount', 'paid_amount']
      __p18_group_col = ['policy_number', 'year', 'diagnosis']
      __p18_claimants_col = ['policy_number', 'year', 'diagnosis', 'claimant']
      __p18_claims_col = ['policy_number', 'year', 'diagnosis', 'claim_id']
      __p18_sort_col = ['policy_number', 'year', 'paid_amount']
      __p18_sort_order = [True, True, False]
    else: 
      __p18_df_col = ['policy_number', 'year'] + by + ['diagnosis', 'incurred_amount', 'paid_amount']
      __p18_group_col = ['policy_number', 'year'] + by + ['diagnosis']
      __p18_claimants_col = ['policy_number', 'year'] + by + ['diagnosis', 'claimant']
      __p18_claims_col = ['policy_number', 'year'] + by + ['diagnosis', 'claim_id']
      __p18_sort_col = ['policy_number', 'year'] + by + ['paid_amount']
      __p18_sort_order = [True, True] + len(by) * [True] + [False]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p18b_df = self.mcr_df[__p18_df_col].loc[(self.mcr_df['benefit_type'] == 'Clinic')].groupby(by=__p18_group_col).sum()
    p18b_df_claimant = self.mcr_df[__p18_claimants_col].loc[(self.mcr_df['benefit_type'] == 'Clinic')].drop_duplicates(subset=['policy_number', 'year', 'diagnosis', 'claimant'], keep='first').groupby(by=__p18_group_col).count().rename(columns={'claimant': 'no_of_claimants'})
    p18b_df['no_of_claimants'] = p18b_df_claimant['no_of_claimants']
    p18b_df_claims = self.mcr_df[__p18_claims_col].loc[(self.mcr_df['benefit_type'] == 'Clinic')].drop_duplicates(subset=['policy_number', 'year', 'diagnosis', 'claim_id']).groupby(by=__p18_group_col).count().rename(columns={'claim_id': 'no_of_claims'})
    p18b_df['no_of_claims'] = p18b_df_claims['no_of_claims']
    p18b_df = p18b_df[['no_of_claimants', 'no_of_claims', 'incurred_amount', 'paid_amount']]
    # p18a_df = p18a_df.unstack().stack(dropna=False)
    if len(p18b_df) > 0:
      p18b_df.sort_values(by=__p18_sort_col, ascending=__p18_sort_order, inplace=True)
    self.p18b = p18b_df
    return p18b_df
  
  def mcr_p27_ts(self, by=None):
    if by == None:
      __p27_df_col = ['policy_number', 'year', 'incur_date', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p27_group_col = ['policy_number', 'year', pd.Grouper(key='incur_date', freq='MS'), 'benefit_type']
      __p27_sort_order = [True, True, True]
    else: 
      __p27_df_col = ['policy_number', 'year'] + by + ['incur_date', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p27_group_col = ['policy_number', 'year'] + by + [pd.Grouper(key='incur_date', freq='MS'), 'benefit_type']
      __p27_sort_order = [True, True] + len(by) * [True] + [True]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p27_df = self.mcr_df[__p27_df_col].groupby(by=__p27_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p27_df = p27_df.unstack()
    p27_df.sort_index(ascending=__p27_sort_order, inplace=True)
    self.p27 = p27_df
    return p27_df
  
  def mcr_p27_ts_op(self, by=None):
    if by == None:
      __p27_df_col = ['policy_number', 'year', 'incur_date', 'benefit', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p27_group_col = ['policy_number', 'year', pd.Grouper(key='incur_date', freq='MS'), 'benefit']
      __p27_sort_order = [True, True, True]
    else: 
      __p27_df_col = ['policy_number', 'year'] + by + ['incur_date', 'benefit', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p27_group_col = ['policy_number', 'year'] + by + [pd.Grouper(key='incur_date', freq='MS'), 'benefit']
      __p27_sort_order = [True, True] + len(by) * [True] + [True]


    self.mcr_df['year'] = self.mcr_df.policy_start_date.dt.year
    p27_df_op = self.mcr_df.loc[(self.mcr_df['benefit_type'] == 'Dental') | (self.mcr_df['benefit_type'] == 'Optical') | (self.mcr_df['benefit'].str.contains('vaccin|check|combined', case=False)) | (self.mcr_df['benefit_type'] == 'Clinic')].copy(deep=True)
    p27_df_op = p27_df_op[__p27_df_col].groupby(by=__p27_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p27_df_op = p27_df_op.unstack()
    p27_df_op.sort_index(ascending=__p27_sort_order, inplace=True)
    self.p27_op = p27_df_op
    return p27_df_op

  def mcr_p28_hosp(self, by=None):
    if by == None:
      __p28_df_col = ['policy_number', 'year', 'hospital_name', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year', 'hospital_name', 'benefit_type']
      __p28_sort_order = [True, True, True]
    else: 
      __p28_df_col = ['policy_number', 'year'] + by + ['hospital_name', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year'] + by + ['hospital_name', 'benefit_type']
      __p28_sort_order = [True, True] + len(by) * [True] + [True]

    p28_df_hosp = self.mcr_df[__p28_df_col].copy(deep=True)
    p28_df_hosp['hospital_name'].fillna('No Hospital Name', inplace=True)
    p28_df_hosp = p28_df_hosp.groupby(by=__p28_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28_df_hosp = p28_df_hosp.unstack()
    p28_df_hosp.sort_index(ascending=__p28_sort_order, inplace=True)
    self.p28_hosp = p28_df_hosp
    return p28_df_hosp

  def mcr_p28_prov(self, by=None):
    if by == None:
      __p28_df_prov_col = ['policy_number', 'year', 'provider', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year', 'provider', 'benefit_type']
      __p28_sort_order = [True, True, True]
    else: 
      __p28_df_prov_col = ['policy_number', 'year'] + by + ['provider', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year'] + by + ['provider', 'benefit_type']
      __p28_sort_order = [True, True] + len(by) * [True] + [True]

    p28_df_prov = self.mcr_df[__p28_df_prov_col].copy(deep=True)
    p28_df_prov['provider'].fillna('No Provider Name', inplace=True)
    p28_df_prov = p28_df_prov.groupby(by=__p28_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28_df_prov = p28_df_prov.unstack()
    p28_df_prov.sort_index(ascending=__p28_sort_order, inplace=True)
    self.p28_prov = p28_df_prov
    return p28_df_prov

  def mcr_p28_doct(self, by=None):
    if by == None:
      __p28_df_doct_col = ['policy_number', 'year', 'physician', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year', 'physician', 'benefit_type']
      __p28_sort_order = [True, True, True]
    else: 
      __p28_df_doct_col = ['policy_number', 'year'] + by + ['physician', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year'] + by + ['physician', 'benefit_type']
      __p28_sort_order = [True, True] + len(by) * [True] + [True]

    p28_df_doct = self.mcr_df[__p28_df_doct_col].copy(deep=True)
    p28_df_doct['physician'].fillna('No Physician Name', inplace=True)
    p28_df_doct = p28_df_doct.groupby(by=__p28_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28_df_doct = p28_df_doct.unstack()
    p28_df_doct.sort_index(ascending=__p28_sort_order, inplace=True)
    self.p28_doct = p28_df_doct
    return p28_df_doct
  
  def mcr_p28a_doct(self, by=None):
    if by == None:
      __p28a_df_doct_col = ['policy_number', 'year', 'physician', 'benefit_type', 'benefit', 'incurred_amount', 'paid_amount', 'claim_id', 'claimant']
      __p28a_group_col = ['policy_number', 'year', 'physician', 'benefit_type', 'benefit']
      __p28a_sort_order = [True, True, True]
    else: 
      __p28a_df_doct_col = ['policy_number', 'year'] + by + ['physician', 'benefit_type', 'benefit', 'incurred_amount', 'paid_amount', 'claim_id', 'claimant']
      __p28a_group_col = ['policy_number', 'year'] + by + ['physician', 'benefit_type', 'benefit']
      __p28a_sort_order = [True, True] + len(by) * [True] + [True]

    p28a_df_doct = self.mcr_df[__p28a_df_doct_col].copy(deep=True)
    p28a_df_doct['physician'].fillna('No Physician Name', inplace=True)
    p28a_df_doct = p28a_df_doct.groupby(by=__p28a_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28a_df_doct['incurred_amount_per_claim'] = p28a_df_doct['incurred_amount'] / p28a_df_doct['no_of_claim_id']
    p28a_df_doct['paid_amount_per_claim'] = p28a_df_doct['paid_amount'] / p28a_df_doct['no_of_claim_id']
    p28a_df_doct.sort_index(ascending=__p28a_sort_order, inplace=True)
    self.p28a_doct = p28a_df_doct
    return p28a_df_doct

  def mcr_p28_proce(self, by=None):
    if by == None:
      __p28_df_proce_col = ['policy_number', 'year', 'procedure', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year', 'procedure', 'benefit_type']
      __p28_sort_order = [True, True, True, True]
    else: 
      __p28_df_proce_col = ['policy_number', 'year'] + by + ['procedure', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year'] + by + ['procedure', 'benefit_type']
      __p28_sort_order = [True, True] + len(by) * [True] + [True, True]

    p28_df_proce = self.mcr_df[__p28_df_proce_col].copy(deep=True)
    p28_df_proce['procedure'].fillna('No Procedure Name', inplace=True)
    p28_df_proce = p28_df_proce.groupby(by=__p28_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28_df_proce = p28_df_proce.unstack()
    p28_df_proce.sort_index(ascending=__p28_sort_order, inplace=True)
    self.p28_proce = p28_df_proce
    return p28_df_proce

  def mcr_p28_proce_diagnosis(self, by=None):
    if by == None:
      __p28_df_proce_diagnosis_col = ['policy_number', 'year', 'procedure', 'diagnosis', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year', 'diagnosis', 'procedure', 'benefit_type']
      __p28_sort_order = [True, True, True, True, True]
    else: 
      __p28_df_proce_diagnosis_col = ['policy_number', 'year'] + by + ['diagnosis', 'procedure', 'benefit_type', 'incurred_amount', 'claim_id', 'paid_amount', 'claimant']
      __p28_group_col = ['policy_number', 'year'] + by + ['diagnosis', 'procedure', 'benefit_type']
      __p28_sort_order = [True, True] + len(by) * [True] + [True, True, True]

    p28_df_proce_diagnosis = self.mcr_df[__p28_df_proce_diagnosis_col].copy(deep=True)
    p28_df_proce_diagnosis['procedure'].fillna('No Procedure Name', inplace=True)
    p28_df_proce_diagnosis = p28_df_proce_diagnosis.groupby(by=__p28_group_col).agg({'incurred_amount': 'sum', 'paid_amount': 'sum', 'claim_id': 'nunique', 'claimant': 'nunique'}).rename(columns={'claim_id': 'no_of_claim_id', 'claimant': 'no_of_claimants'})
    p28_df_proce_diagnosis = p28_df_proce_diagnosis.unstack()
    p28_df_proce_diagnosis.sort_index(ascending=__p28_sort_order, inplace=True)
    self.p28_proce_diagnosis = p28_df_proce_diagnosis
    return p28_df_proce_diagnosis


  def mcr_pages(self, by=None, export=False, benefit_type_order=['Hospital', 'Clinic', 'Dental', 'Optical', 'Maternity', 'Total']):
    
    if type(by) is not list and by != None: by = [by]

    self.mcr_df = self.df.copy(deep=True)
    if type(by) is list:
      if 'dep_type' in by:
        self.mcr_df['dep_type'].loc[(self.mcr_df['dep_type'].str.contains('CH')) | (self.mcr_df['dep_type'].str.contains('SP'))] = 'DEP'
    
    self.mcr_p20_policy(by)
    self.mcr_p20_benefit(by, benefit_type_order)
    self.mcr_p20_panel(by)
    self.mcr_p20_panel_clin(by)
    self.mcr_p21_class(by)
    self.mcr_p22_class_benefit(by, benefit_type_order)
    self.mcr_p23_ip_benefit(by)
    self.mcr_p23a_class_ip_benefit(by)
    self.mcr_p24_op_benefit(by)
    self.mcr_p24a_op_class_benefit(by)
    self.mcr_p24_dent_benefit(by)
    self.mcr_p24_wellness_benefit(by)
    self.mcr_p24_class_wellness_benefit(by)
    self.mcr_p25_class_panel_benefit(by, benefit_type_order)
    self.mcr_p26_op_panel(by)
    self.mcr_p26_op_class_panel(by)
    self.mcr_p26_ip_panel(by)
    self.mcr_p27_ts(by)
    self.mcr_p27_ts_op(by)
    self.mcr_p28_hosp(by)
    self.mcr_p28_prov(by)
    self.mcr_p28_doct(by)
    self.mcr_p28a_doct(by)
    self.mcr_p28_proce(by)
    self.mcr_p28_proce_diagnosis(by)
    #self.mcr_p28_class_dep_ip_benefit(by)
    self.mcr_p18a_top_diag_ip(by)
    self.mcr_p18b_top_diag_op(by)

    if export == True:
      from io import BytesIO
      output = BytesIO()
      # mcr_filename = 'mcr.xlsx'
      mcr_filename = output
      with pd.ExcelWriter(mcr_filename) as writer:
        self.p20_policy.to_excel(writer, sheet_name='P.20_Policy', index=True, merge_cells=False)
        self.p20.to_excel(writer, sheet_name='P.20_BenefitType', index=True, merge_cells=False)
        self.p20_panel.to_excel(writer, sheet_name='P.20_Network', index=True, merge_cells=False)
        self.p20_panel_clin.to_excel(writer, sheet_name='P.20_Network_Clinic', index=True, merge_cells=False)
        self.p21.to_excel(writer, sheet_name='P.21_Class', index=True, merge_cells=False)
        self.p22.to_excel(writer, sheet_name='P.22_Class_BenefitType', index=True, merge_cells=False)
        self.p23.to_excel(writer, sheet_name='P.23_IP_Benefit', index=True, merge_cells=False)
        self.p23a.to_excel(writer, sheet_name='P.23a_Class_IP_Benefit', index=True, merge_cells=False)
        self.p24.to_excel(writer, sheet_name='P.24_OP_Benefit', index=True, merge_cells=False)
        self.p24a.to_excel(writer, sheet_name='P.24a_Class_OP_Benefit', index=True, merge_cells=False)
        self.p24_dent_benefit.to_excel(writer, sheet_name='P.24d_Dental', index=True, merge_cells=False)
        self.p24_wellness_benefit.to_excel(writer, sheet_name='P.24w_Wellness', index=True, merge_cells=False)
        self.p24_class_wellness_benefit.to_excel(writer, sheet_name='P.24wc_Class_Wellness', index=True, merge_cells=False)
        self.p25.to_excel(writer, sheet_name='P.25_Class_Panel_BenefitType', index=True, merge_cells=False)
        self.p26.to_excel(writer, sheet_name='P.26_OP_Panel_Benefit', index=True, merge_cells=False)
        self.p26_op_class_panel.to_excel(writer, sheet_name='P.26a_OP_Class_Panel_Benefit', index=True, merge_cells=False)
        self.p26_ip_panel.to_excel(writer, sheet_name='P.26b_IP_Panel_Benefit', index=True, merge_cells=False)
        self.p27.to_excel(writer, sheet_name='P.27_TimeSeries', index=True, merge_cells=False)
        self.p27_op.to_excel(writer, sheet_name='P.27a_TimeSeries_OP', index=True, merge_cells=False)
        self.p28_hosp.to_excel(writer, sheet_name='P.28_Hospital', index=True, merge_cells=False)
        self.p28_prov.to_excel(writer, sheet_name='P.28_Provider', index=True, merge_cells=False)
        self.p28_doct.to_excel(writer, sheet_name='P.28_Physician', index=True, merge_cells=False)
        self.p28a_doct.to_excel(writer, sheet_name='P.28a_Physician_Benefit', index=True, merge_cells=False)
        self.p28_proce.to_excel(writer, sheet_name='P.28_Procedures', index=True, merge_cells=False)
        self.p28_proce_diagnosis.to_excel(writer, sheet_name='P.28a_Procedures_Diag', index=True, merge_cells=False)
        #self.p28.to_excel(writer, sheet_name='P.28_Class_Dep_IP_Benefit', index=True, merge_cells=False)
        self.p18a.to_excel(writer, sheet_name='P.18_TopHosDiag', index=True, merge_cells=False)
        self.p18b.to_excel(writer, sheet_name='P.18a_TopClinDiag', index=True, merge_cells=False)
        writer.close()
        # processed_data = output.getvalue()
        return output.getvalue()


  def mcr_p20_ibnr(self, ibnr=1, no_month=9):

    self.p20_ibnr = self.p20['paid_amount']
    self.p20_ibnr['paid_ibnr'] = self.p20_ibnr['paid_amount'] * ibnr
    self.p20_ibnr['paid_amount_annu'] = self.p20_ibnr['paid_amount'] * 12 / no_month
    self.p20_ibnr['paid_ibnr_annu'] = self.p20_ibnr['paid_ibnr'] * 12 / no_month


  def time_comparison(self, start_m = None, policy_number=None, sub_policy=False):
    if start_m == None:
      start_m = str(self.df.policy_start_date.iloc[0].dt.month)
    order = [str(i) for i in np.arange(start_m, 13).tolist()] + [str(j) for j in np.arange(1, start_m).tolist()]
    # print(order)
    self.df['year'] = self.df.policy_start_date.dt.year
    self.df['month'] = self.df.incur_date.dt.month.astype(str)
    time_com = self.df[['policy_number', 'year', 'month', 'paid_amount']].groupby(by=['policy_number', 'year', 'month']).sum()

    if policy_number == None:
      policy_list = time_com.index.get_level_values(0).unique()
    else:
      policy_list = policy_number

    year_list = time_com.index.get_level_values(1).unique()

    self.by_time, self.by_time_ax = plt.subplots(figsize=(15, 5))

    for p in policy_list:
      for y in year_list:
        __tdf = time_com.loc[p, y].reindex(order)
        self.by_time_ax.plot(__tdf.index, __tdf.paid_amount, marker='o', label=f'{p}__{y}', linewidth=2, markersize=6)
        self.by_time_ax.set_title('Monthly Trend Claim Paid Comparison', fontsize=18)
        # self.by_time_ax.set_ylim(0, 1_500_000)
        self.by_time_ax.set_xticks(order)
        self.by_time_ax.set_xlabel('Month')
        self.by_time_ax.set_ylabel('Claim Paid Amount')
        self.by_time_ax.legend()



      # self.whatever_df.index.strftime('%b')
    # return self.by_time

  def frequent_claimant_analysis(self, export=True, sub_policy=None):
    freq_op_list = ['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)', 'Diagnostic: X-Ray & Lab Test (DX)', 'Prescribed Medicine (PM)']
    total_visit_col = ['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)']
    self.df.policy_start_date = pd.to_datetime(self.df.policy_start_date)
    self.df['year'] = self.df.policy_start_date.dt.year
    self.df.claimant = self.df.policy_id.str.cat(self.df.claimant, sep='_')
    __dep = self.df
    # __dep.claimant = __dep.policy_number.str.cat(__dep.year.astype(str), sep='_').str.cat(__dep.claimant, sep='_')
    __dep = __dep[['claimant', 'dep_type', 'class', 'suboffice', 'age', 'gender']]
    __dep.drop_duplicates(subset=['claimant', 'suboffice'], keep='first', inplace=True)
    __freq_df = self.df[['policy_number', 'year', 'claimant', 'benefit', 'incur_date']].dropna()
    __freq_df = __freq_df.loc[__freq_df.benefit.isin(freq_op_list)].groupby(['policy_number', 'year', 'claimant', 'benefit']).count()
    # print(__freq_df)
    __freq_df = __freq_df.unstack()
    __dep.index = __dep['claimant'].values.tolist()
    __freq_df.columns = __freq_df.columns.droplevel()

    for col in total_visit_col:
      if col not in __freq_df.columns:
        __freq_df[col] = np.nan
    __freq_df['total_claims'] = __freq_df[total_visit_col].sum(axis=1)
    # __freq_df = __freq_df.sort_values(by=['policy_number', 'year', 'total_claims'], ascending=False)
    # print(__dep)
    # print(__freq_df)
    #__freq_df = pd.merge(left=__freq_df, right=__dep, left_index=True, right_index=True, how='left')
    __freq_df = pd.merge(left=__freq_df, right=__dep, left_on='claimant', right_index=True, how='left').drop(columns=['claimant'])
    __freq_df = __freq_df.sort_values(by=['policy_number', 'year', 'total_claims'], ascending=[True, True, False])

    __freq_df['GP + SP'] = __freq_df[['General Consultation (GP)', 'Specialist Consultation (SP)']].sum(axis=1)
    __freq_df['Physio + Chiro'] = __freq_df[['Chiro (CT)', 'Physio (PT)']].sum(axis=1)

    __freq_df = __freq_df.reset_index()
    __freq_sum_benefit_df = self.df[['claimant', 'benefit', 'paid_amount']].loc[self.df.benefit.isin(total_visit_col)].groupby(['claimant', 'benefit']).mean().rename(columns={'paid_amount': 'total_paid'}).unstack().stack(dropna=False)
    __freq_sum_benefit_df = __freq_sum_benefit_df.reindex(total_visit_col, level='benefit').unstack()
    print(__freq_sum_benefit_df.columns)
    __freq_sum_benefit_df.columns = [f'{b}_paid_per_claim' for b in total_visit_col]
    __freq_inc_benefit_df = self.df[['claimant', 'benefit', 'incurred_amount']].loc[self.df.benefit.isin(total_visit_col)].groupby(['claimant', 'benefit']).mean().rename(columns={'incurred_amount': 'total_incurred'}).unstack().stack(dropna=False)
    __freq_inc_benefit_df = __freq_inc_benefit_df.reindex(total_visit_col, level='benefit').unstack()
    __freq_inc_benefit_df.columns = [f'{b}_incurred_per_claim' for b in total_visit_col]
    __freq_sum_df = self.df[['claimant', 'paid_amount']].loc[self.df.benefit.isin(total_visit_col)].groupby(['claimant']).sum().rename(columns={'paid_amount': 'total_paid'})
    __freq_df = pd.merge(left=__freq_df, right=__freq_inc_benefit_df, left_on='claimant', right_on='claimant', how='left')
    __freq_df = pd.merge(left=__freq_df, right=__freq_sum_benefit_df, left_on='claimant', right_on='claimant', how='left')
    __freq_df = pd.merge(left=__freq_df, right=__freq_sum_df, left_on='claimant', right_on='claimant', how='left')
    __freq_dx_df = self.df[['claimant', 'incurred_amount', 'paid_amount']].loc[self.df.benefit.str.contains('DX', case=False)].groupby(['claimant']).agg({'incurred_amount': 'sum', 'paid_amount': 'sum'}).rename(columns={'incurred_amount': 'Diagnostic: X-Ray & Lab Test (DX)_incurred', 'paid_amount': 'Diagnostic: X-Ray & Lab Test (DX)_paid'})
    __freq_df = pd.merge(left=__freq_df, right=__freq_dx_df, left_on='claimant', right_on='claimant', how='left')
    __freq_pm_df = self.df[['claimant', 'incurred_amount', 'paid_amount']].loc[self.df.benefit.str.contains('PM', case=False)].groupby(['claimant']).agg({'incurred_amount': 'sum', 'paid_amount': 'sum'}).rename(columns={'incurred_amount': 'Prescribed Medicine (PM)_incurred', 'paid_amount': 'Prescribed Medicine (PM)_paid'})
    __freq_df = pd.merge(left=__freq_df, right=__freq_pm_df, left_on='claimant', right_on='claimant', how='left')
    __freq_df['paid_per_claim'] = __freq_df['total_paid'] / __freq_df['total_claims']
    __freq_df['Diagnostic: X-Ray & Lab Test (DX)_paid_per_claim'] = __freq_df['Diagnostic: X-Ray & Lab Test (DX)_paid'] / __freq_df['Diagnostic: X-Ray & Lab Test (DX)']
    __freq_df['Prescribed Medicine (PM)_paid_per_claim'] = __freq_df['Prescribed Medicine (PM)_paid'] / __freq_df['Prescribed Medicine (PM)']
    __freq_df = __freq_df.set_index(['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender'])
    # __freq_df.reset_index(inplace=True)
    __freq_df = __freq_df[['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)',
                            'total_claims', 'total_paid', 'paid_per_claim', 'GP + SP', 'Physio + Chiro', 
                            'Diagnostic: X-Ray & Lab Test (DX)', 'Diagnostic: X-Ray & Lab Test (DX)_incurred', 'Diagnostic: X-Ray & Lab Test (DX)_paid', 'Diagnostic: X-Ray & Lab Test (DX)_paid_per_claim',
                            'Prescribed Medicine (PM)', 'Prescribed Medicine (PM)_incurred', 'Prescribed Medicine (PM)_paid', 'Prescribed Medicine (PM)_paid_per_claim',
                            'General Consultation (GP)_incurred_per_claim', 'Specialist Consultation (SP)_incurred_per_claim', 'Chinese Med (CMT)_incurred_per_claim', 
                            'Chiro (CT)_incurred_per_claim', 'Physio (PT)_incurred_per_claim',
                            'General Consultation (GP)_paid_per_claim', 'Specialist Consultation (SP)_paid_per_claim', 'Chinese Med (CMT)_paid_per_claim', 
                            'Chiro (CT)_paid_per_claim', 'Physio (PT)_paid_per_claim']]

    self.frequent_analysis = __freq_df

    def descriptive_stats(data):
    # Calculate statistics
      stats = data.describe()
      # Add median to the statistics
      stats.loc['median'] = data.median()
      stats.loc['90%'] = data.quantile(q=0.9)
      stats.loc['95%'] = data.quantile(q=0.95)
      stats.loc['97%'] = data.quantile(q=0.97)
      stats.loc['99%'] = data.quantile(q=0.99)
      # Reorder the statistics
      stats = stats.reindex(['count', 'mean', 'median', 'std', 'min', '25%', '50%', '75%', '90%', '95%', '97%', '99%', 'max'])
      return stats


    numeric_columns = __freq_df.select_dtypes(include=[np.number]).columns
    order_cols = ['year'] + numeric_columns.tolist()
    __freq_stat_df = pd.DataFrame()
    for y in __freq_df.index.get_level_values(level='year').unique():
      __temp_df = pd.DataFrame()
      for column in numeric_columns:
        # print(f"\nDescriptive Statistics for {column}:")
        __temp_df = pd.concat([__temp_df, descriptive_stats(__freq_df[column].loc[__freq_df.index.get_level_values(level='year') == y])], axis=1, ignore_index=False)
      __temp_df['year'] = y
      __temp_df = __temp_df[order_cols]
      __freq_stat_df = pd.concat([__freq_stat_df, __temp_df], axis=0)

    self.frequent_analysis_stat = __freq_stat_df

    days = self.df.loc[self.df.benefit.str.contains('room', case=False)]
    days['days_cover'] = (days['discharge_date'] - days['incur_date']).dt.days + 1

    days_cover = days[['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'diagnosis', 'days_cover']] \
    .loc[self.df.benefit_type.str.contains('hosp', case=False)] \
    .groupby(by=['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'diagnosis']).sum()


    self.ip_usage = self.df[['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'benefit', 'diagnosis', 'paid_amount']] \
    .loc[self.df.benefit_type.str.contains('hosp', case=False)] \
    .groupby(by=['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'diagnosis', 'benefit']).sum().unstack()
    cols = [list(f)[-1] for f in self.ip_usage.columns]
    self.ip_usage.columns = cols

   

    self.ip_usage_incurred = self.df[['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'benefit', 'diagnosis', 'incurred_amount']] \
    .loc[self.df.benefit_type.str.contains('hosp', case=False)] \
    .groupby(by=['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'diagnosis', 'benefit']).sum().unstack()
    cols = [list(f)[-1] for f in self.ip_usage_incurred.columns]
    self.ip_usage_incurred.columns = cols

    self.ip_usage_incurred = pd.concat([self.ip_usage_incurred, days_cover], axis=1, ignore_index=False)

    days_cover = days[['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'benefit', 'diagnosis', 'days_cover']] \
    .loc[self.df.benefit_type.str.contains('hosp', case=False)] \
    .groupby(by=['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'gender', 'diagnosis', 'benefit']).sum()

    # self.ip_usage_for_cal = self.df[['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'benefit', 'diagnosis', 'incurred_amount', 'paid_amount']] \
    # .loc[self.df.benefit_type.str.contains('hosp', case=False)] \
    # .groupby(by=['policy_number', 'year', 'suboffice', 'claimant', 'class', 'dep_type', 'age', 'diagnosis', 'benefit']).sum()

    # self.ip_usage_for_cal = pd.concat([self.ip_usage_for_cal, days_cover], axis=1, ignore_index=False)

    

    if export == True:
      from io import BytesIO
      output = BytesIO()
      # mcr_filename = 'mcr.xlsx'
      freq_claimant_file = output
      with pd.ExcelWriter(freq_claimant_file) as writer_2:
        self.frequent_analysis.to_excel(writer_2, sheet_name='Claimant Visits', index=True, merge_cells=False)
        self.frequent_analysis_stat.to_excel(writer_2, sheet_name='Claimant Statistics', index=True, merge_cells=False)
        self.ip_usage.to_excel(writer_2, sheet_name='Claimant IP Usage', index=True, merge_cells=False)
        self.ip_usage_incurred.to_excel(writer_2, sheet_name='Claimant IP Usage Incurred', index=True, merge_cells=False)
        # self.ip_usage_for_cal.to_excel(writer_2, sheet_name='Claimant IP Usage for Calculation', index=True, merge_cells=False)
        writer_2.close()
      return output.getvalue()

  def fair_ibnr_estimation(self, months=9):
    __fair_ibnr_df = self.df.copy(deep=True)
    __fair_ibnr_df['year']  = __fair_ibnr_df['policy_start_date'].dt.year.astype(str)
    __fair_ibnr_df['ibnr_date'] = __fair_ibnr_df['policy_start_date'] + pd.DateOffset(months=months)
    __fair_ibnr_df['ibnr_paid'] = 0
    __fair_ibnr_df['ibnr_paid'].loc[(__fair_ibnr_df['ibnr_date'] <= __fair_ibnr_df['submission_date']) & (__fair_ibnr_df['ibnr_date'] > __fair_ibnr_df['incur_date'])] = __fair_ibnr_df['paid_amount'].loc[(__fair_ibnr_df['ibnr_date'] <= __fair_ibnr_df['submission_date']) & (__fair_ibnr_df['ibnr_date'] > __fair_ibnr_df['incur_date'])]
    __fair_ibnr_df['reported_paid'] = 0
    __fair_ibnr_df['reported_paid'].loc[(__fair_ibnr_df['ibnr_date'] > __fair_ibnr_df['submission_date'])] = __fair_ibnr_df['paid_amount'].loc[(__fair_ibnr_df['ibnr_date'] > __fair_ibnr_df['submission_date'])]
    __fair_ibnr_df['hospital_ibnr_paid'], __fair_ibnr_df['hospital_reported_paid'], __fair_ibnr_df['clinic_ibnr_paid'], __fair_ibnr_df['clinic_reported_paid'] = 0, 0, 0, 0
    __fair_ibnr_df['hospital_ibnr_paid'].loc[__fair_ibnr_df.benefit_type == 'Hospital'] = __fair_ibnr_df['ibnr_paid'].loc[__fair_ibnr_df.benefit_type == 'Hospital']
    __fair_ibnr_df['hospital_reported_paid'].loc[__fair_ibnr_df.benefit_type == 'Hospital'] = __fair_ibnr_df['reported_paid'].loc[__fair_ibnr_df.benefit_type == 'Hospital']
    __fair_ibnr_df['clinic_ibnr_paid'].loc[__fair_ibnr_df.benefit_type == 'Clinic'] = __fair_ibnr_df['ibnr_paid'].loc[__fair_ibnr_df.benefit_type == 'Clinic']
    __fair_ibnr_df['clinic_reported_paid'].loc[__fair_ibnr_df.benefit_type == 'Clinic'] = __fair_ibnr_df['reported_paid'].loc[__fair_ibnr_df.benefit_type == 'Clinic']
    
    fair_ibnr = __fair_ibnr_df[['policy_number', 'year', 'reported_paid', 'ibnr_paid', 'hospital_reported_paid', 'hospital_ibnr_paid', 'clinic_reported_paid', 'clinic_ibnr_paid']].groupby(by=['policy_number', 'year']).sum()
    fair_ibnr['ibnr_rate'] = fair_ibnr['ibnr_paid'] / fair_ibnr['reported_paid']
    fair_ibnr['hospital_ibnr_rate'] = fair_ibnr['hospital_ibnr_paid'] / fair_ibnr['hospital_reported_paid']
    fair_ibnr['clinic_ibnr_rate'] = fair_ibnr['clinic_ibnr_paid'] / fair_ibnr['clinic_reported_paid']
    fair_ibnr = fair_ibnr[['reported_paid', 'ibnr_paid', 'ibnr_rate', 'hospital_reported_paid', 'hospital_ibnr_paid', 'hospital_ibnr_rate', 'clinic_reported_paid', 'clinic_ibnr_paid', 'clinic_ibnr_rate']]
    self.fair_ibnr = fair_ibnr
    return fair_ibnr

  def make_autopct(values):
    def my_autopct(pct):
        total = np.sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.1f}%  ({v:,})'.format(p=pct,v=val)
    return my_autopct

  ################################################################################################
  # this function should modify into incident rate
  ################################################################################################

  def benefit_op_monthly(self, benefit=['GP', 'CMT', 'SP', 'PM', 'CT', 'PT', 'DX', 'Psy', 'Vac', 'Check']):
    self.df['year'] = self.df.policy_start_date.dt.year
    __benefit_df = self.df[['benefit', 'incur_date', 'policy_id']].loc[self.df['benefit'].str.contains('|'.join(benefit), case=True)].groupby(['benefit', pd.Grouper(key='incur_date', freq='m')]).count().rename(columns={'policy_id': 'no_of_claims'})

    __benefit_l = __benefit_df.index.get_level_values(level='benefit').unique().tolist()

    __tdf = pd.DataFrame()
    for b in __benefit_l:
      __tdf = pd.concat([__tdf, __benefit_df.loc[b].rename(columns={'no_of_claims': b})], axis=1, ignore_index=False)

    # self.benefit_trend = __benefit_fig

    
    df = __tdf
    fig = px.line(df, x=df.index, y=df.columns,
                #hover_data={df.index: "|%B %d, %Y"},
                markers=True,
                width=1200,
                height=600,
                title='Monthly Number of visit of Outpatient Benefit',
                )
    fig.update_layout(
      xaxis_title='Month',
      
      yaxis_title='No. of visits',
      yaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=25
      ),
      legend=dict(
        orientation='h',
        #yanchor='bottom',
        #y=1.02
      )
    )
    
    # fig.update_xaxes(
    # dtick="M1",
    # tickformat="%b\n%Y")

    return fig

  def benefit_op_yearly_bar(self, benefit=['GP', 'CMT', 'SP', 'PM', 'CT', 'PT', 'DX', 'Psy', 'Vac', 'Check']):
    self.df['year'] = self.df.policy_start_date.dt.year
    __benefit_df = self.df[['benefit', 'policy_id', 'year']].loc[self.df['benefit'].str.contains('|'.join(benefit), case=True)].groupby(['benefit', 'policy_id']).count().rename(columns={'year': 'no_of_claims'})
    __benefit_df  =__benefit_df.astype(int)
    __benefit_df.unstack().stack(dropna=False)
    __benefit_df.fillna(0, inplace=True)
    __benefit_l = __benefit_df.index.get_level_values(level='benefit').unique().tolist() #v
    __policy_id_l = __benefit_df.index.get_level_values(level='policy_id').unique().tolist() #x

    fig = go.Figure()
    for b in __benefit_l:
      fig.add_trace(go.Bar(
        x=__policy_id_l,
        y=__benefit_df['no_of_claims'].loc[b].values.tolist(),
        #y=[1,2,3],
        name=b,
        
      ))
      #print(__benefit_df.loc[b].values.tolist())


    fig.update_layout(
      barmode='group',
      title=dict(
        text='Number of visit of Outpatient Benefit',
        font=dict(size=28),
                 ),
      # title_x = 0.5,
      xaxis_title='Policy',
      yaxis_title='No. of Visit',
      yaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=100
      ),
      legend=dict(
        orientation='h',
        #yanchor='bottom',
        #y=1.02
      ),
      width=1200,
      height=600,
    )
    
    return fig


  def class_age_scatter(self):
    self.df['year'] = self.df.policy_start_date.dt.year
    __class_age_df = self.df[['class', 'age', 'incurred_amount']].loc[self.df['benefit_type'].str.contains('hosp|top|smm', case=False)]
    __class_age_df['age']  = __class_age_df['age'].astype(int)
    __class_l = __class_age_df['class'].unique()

    fig = go.Figure()
    for cls in __class_l:
      fig.add_trace(go.Scatter(
        x=__class_age_df['age'].loc[__class_age_df['class'] == cls],
        y=__class_age_df['incurred_amount'].loc[__class_age_df['class'] == cls],
        mode='markers',
        #y=[1,2,3],
        name=cls,
        
      ))
      #print(__benefit_df.loc[b].values.tolist())


    fig.update_layout(
      title=dict(
        text='Relationship of age and incurred amount of inpatient',
        font=dict(size=28),
                 ),
      # title_x = 0.5,
      xaxis_title='Age',
      yaxis_title='Incurred Amount',
      yaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=10_000
      ),
      legend=dict(
        orientation='h',
        #yanchor='bottom',
        #y=1.02
      ),
      width=1200,
      height=600,
    )
    
    return fig
  

  def paid_amount_by_dep_type_policy_year(self):
    
    self.df['year'] = self.df.policy_start_date.dt.year

    __temp_df = self.df[['policy_id','dep_type','paid_amount']].replace({'CH': 'DEP', 'SP': 'DEP'})
    __frequency_counts = self.df.groupby(['policy_id', 'dep_type']).count().rename(columns={'paid_amount': 'no_of_claims'})
    __paid_by_dep_plot_df = __temp_df.groupby(['policy_id', 'dep_type']).sum()
    __paid_by_dep_plot_df['no_of_claims'] = __frequency_counts['no_of_claims']
    __paid_by_dep_plot_df = __paid_by_dep_plot_df.reset_index()
    fig = px.bar(__paid_by_dep_plot_df, x='policy_id', y='paid_amount', color='dep_type', barmode='group', hover_data = {'no_of_claims': True})

    fig.update_layout(
      yaxis_title='Paid Amount',
      title=dict(
        text='Paid Amount by Dependent Type by Policy Year',
        font=dict(size=28),
        ),
      # title_x = 0.5,
      yaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=100_000
        ),
      xaxis_type='category',
      # xaxis=dict(
      #   tickmode='array',
      #   tickvals = __paid_by_dep_plot_df['class'].drop_duplicates(keep='first').to_list(),
      # ),
      legend=dict(
        orientation='h',
        
        #yanchor='bottom',
        #y=1.02
        ),
      legend_title_text='Dependent Type',
      width=900,
      height=600,
    )
    # fig.update_xaxes(type='category', categoryorder='category ascending')
    return fig
  
  def paid_amount_by_dep_type_class_policy_year(self):
    self.df['year'] = self.df.policy_start_date.dt.year

    __temp_df = self.df[['policy_id','class','dep_type','paid_amount']].replace({'CH': 'DEP', 'SP': 'DEP'})
    __frequency_counts = self.df.groupby(['policy_id', 'dep_type','class']).count().rename(columns={'paid_amount': 'no_of_claims'})
    __paid_by_dep_class_plot_df = __temp_df.groupby(['policy_id', 'dep_type' ,'class']).sum()
    __paid_by_dep_class_plot_df['no_of_claims'] = __frequency_counts['no_of_claims']
    __paid_by_dep_class_plot_df = __paid_by_dep_class_plot_df.reset_index()
    __paid_by_dep_class_plot_df['class'] = __paid_by_dep_class_plot_df['class'].astype(str)
    fig = px.bar(__paid_by_dep_class_plot_df, x='class', y='paid_amount', color='dep_type', barmode='group',facet_col='policy_id',hover_data = {'no_of_claims': True})

    fig.update_layout(
      yaxis_title='Paid Amount',
      title=dict(
        text='Paid Amount by Dependent Type by Class by Policy Year',
        font=dict(size=28),
                 ),
      # title_x = 0.5,
      yaxis=dict(
        tickmode='linear',
        tick0=0,
        dtick=50_000
      ),
      xaxis_type='category',
      # xaxis=dict(
      #   tickmode='array',
      #   tickvals = __paid_by_dep_plot_df['class'].drop_duplicates(keep='first').to_list(),
      # ),
      legend=dict(
        orientation='h',
        
        #yanchor='bottom',
        #y=1.02
      ),
      legend_title_text='Dependent Type',
      width=1500,
      height=600,
    )
    fig.update_xaxes(type='category', categoryorder='category ascending')
    return fig


  def gender_analysis_by_op(self):
    self.df['year'] = self.df.policy_start_date.dt.year
    sex_panel = self.df[['year', 'gender', 'panel', 'incur_date']].loc[(self.df.benefit == 'General Consultation (GP)') | (self.df.benefit == 'Chinese Med (CMT)') | (self.df.benefit == 'Specialist Consultation (SP)')]
    sex_panel = sex_panel.groupby(by=['year', 'gender', 'panel']).count().rename(columns={'incur_date': 'no_of_claims'})

    __year_l = self.df.year.unique().tolist()
    __gender_l = self.df.gender.unique().tolist()
    __benefit_l = ['General Consultation (GP)', 'Chinese Med (CMT)', 'Specialist Consultation (SP)']


    pie_fig, pie_ax = plt.subplots(nrows=len(__year_l), ncols=len(__gender_l), figsize=(6*len(__year_l), 6*len(__gender_l)))
    ax[0].pie(labels=_plt1.index, x=_plt1.no_of_claims, autopct=self.make_autopct(_plt1.no_of_claims), textprops={'fontsize': 12})
    ax[0].set_title(f'Male, Total no of visit {_plt1.no_of_claims.sum()}')
    ax[1].pie(labels=_plt2.index, x=_plt2.no_of_claims, autopct=self.make_autopct(_plt2.no_of_claims), textprops={'fontsize': 12})
    ax[1].set_title(f'Female, Total no of visit {_plt2.no_of_claims.sum()}')
    fig.suptitle('Panel Usage by Gender of General Consultation, Chinese Medicine, and Specialist')

  def export_database(self):
    return self.df.to_csv(index=False).encode('utf-8')


