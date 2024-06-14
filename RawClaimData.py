
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
        'chornic',
        'currency',
        'incurred_amount',
        'paid_amount',
        'panel',
        'suboffice',
        'region',
    ]

    self.col_setup = col_setup
    self.df = pd.DataFrame(columns=self.col_setup)

    self.benefit_index = pd.read_excel('benefit_indexing.xlsx')


  def __axa_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK'):

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
        # 'chornic', # not available
        'CURRENCY': 'currency',
        'AMOUNT BILLED': 'incurred_amount',
        'AMOUNT PAID': 'paid_amount',
        # 'panel', deduce from benefit code
        'AFFILIATED CO': 'suboffice',
        # 'region',

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
        # 'chornic', # not available
        'CURRENCY': str,
        'AMOUNT BILLED': float,
        'AMOUNT PAID': float,
        # 'panel', deduce from benefit code
        'AFFILIATED CO': str,
        # 'region',

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
        'CERTICFICATE NO': 'claimant',
        'SEX': 'gender',
        # 'age', deduce from DOB
        'DEPENDENT NO': 'dep_type',
        'HEALTH CLASS LEVEL  /OPT. HOSPITAL CLASS': 'class',
        # 'member_status', # not available
        # 'benefit_type', # sheetname
        'BENEFIT CODE': 'benefit', # need process
        # 'diagnosis', # not available
        # 'chornic', # not available
        'CURRENCY': 'currency',
        'AMOUNT BILLED': 'incurred_amount',
        'AMOUNT PAID': 'paid_amount',
        # 'panel', deduce from benefit code
        'AFFILIATED CO': 'suboffice',
        # 'region',

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
        # 'chornic', # not available
        'CURRENCY': str,
        'AMOUNT BILLED': float,
        'AMOUNT PAID': float,
        # 'panel', deduce from benefit code
        'AFFILIATED CO': str,
        # 'region',

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
          df_c['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          df_c['discharge_date'] = df_c['incur_date']
          df_c['cert_true_copy'] = np.nan
          if 'birth_date' in df_c.columns.tolist() and df_c['birth_date'].values[0] != np.nan:
            df_c['age'] = ((df_c['policy_start_date'] - df_c['birth_date']).dt.days/365.25).astype(int)
          else:
            df_c['age'] = np.nan
          df_c['member_status'] = np.nan
          df_c['benefit_type'] = 'Clinic'
          df_c['diagnosis'] = np.nan
          df_c['chornic'] = np.nan
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
          df_d['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          df_d['discharge_date'] = df_d['incur_date']
          df_d['cert_true_copy'] = np.nan
          if 'birth_date' in df_d.columns.tolist() and df_d['birth_date'].values[0] != np.nan:
            df_d['age'] = ((df_d['policy_start_date'] - df_d['birth_date']).dt.days/365.25).astype(int)
          else:
            df_d['age'] = np.nan
          df_d['member_status'] = np.nan
          df_d['benefit_type'] = 'Dental'
          df_d['diagnosis'] = np.nan
          df_d['chornic'] = np.nan
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
          df_h['policy_end_date'] = pd.to_datetime(_policy_end, format='%Y%m%d')
          # df_h['discharge_date'] = df_h['incur_date']
          df_h['cert_true_copy'] = np.nan
          if 'birth_date' in df_h.columns.tolist() and df_h['birth_date'].values[0] != np.nan:
            df_h['age'] = ((df_h['policy_start_date'] - df_h['birth_date']).dt.days/365.25).astype(int)
          else:
            df_d['age'] = np.nan
          df_h['member_status'] = np.nan
          df_h['benefit_type'] = 'Hospital'
          df_h['diagnosis'] = np.nan
          df_h['chornic'] = np.nan
          df_h['panel'] = 'Non-Panel'
          df_h['panel'].loc[df_h.benefit.str.contains('1')] = 'Panel'

          for col in self.col_setup:
            if col not in df_h.columns.tolist():
              df_h[col] = np.nan

      axa_index = self.benefit_index[['gum_benefit', 'axa_benefit_code']]
      t_df = pd.concat([df_c[self.col_setup], df_h[self.col_setup], df_d[self.col_setup]], axis=0, ignore_index=True)
      t_df = pd.merge(left=t_df, right=axa_index, left_on='benefit', right_on='axa_benefit_code', how='left')
      t_df.benefit = t_df.gum_benefit
      t_df.suboffice.fillna('00', inplace=True)
      t_df.diagnosis.fillna('No diagnosis provided', inplace=True)
      t_df['region'] = region
      t_df = t_df[self.col_setup]

    t_df.gender.loc[t_df.gender.str.contains('f', case=False)] = 'F'
    t_df.gender.loc[t_df.gender.str.contains('m', case=False)] = 'M'

    axa_dep_type = {'1': 'EE', '2': 'SP', '3': 'CH', '4': 'CH', '5': 'CH', '6': 'CH', '7': 'CH', '8': 'CH'}
    t_df.dep_type.replace(axa_dep_type, inplace=True)
    # self.df = pd.concat([self.df, t_df], axis=0)
    return t_df

  def __bupa_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK'):

    bupa_rename_col = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        # 'policy_number', # Contract NUmber A1
        # 'insurer', # Bupa
        # 'client_name', # Customer Name A2
        # 'policy_start_date', # Period Cell A3
        # 'policy_end_date',
        'Voucher number': 'claim_id',
        'Incur Date': 'incur_date',
        # 'discharge_date', convert from days cover
        'Receipt date': 'submission_date',
        'Pay date': 'pay_date',
        # 'claim_status',
        # 'claim_remark',
        # 'cert_true_copy',
        'Claimant': 'claimant',
        'Sex': 'gender',
        'Age': 'age',
        'Member type': 'dep_type',
        'Class ID': 'class',
        'Member Status': 'member_status',
        'Benefit Type': 'benefit_type',
        'Benefit': 'benefit',
        'Diagnosis description': 'diagnosis',
        'Chronic flag': 'chornic',
        # 'currency',
        'Presented': 'incurred_amount',
        'Adjusted': 'paid_amount',
        'Network Category': 'panel',
        # 'suboffice', # deduced from contract number columns last 2 digits
        # 'region',

        'Days Cover': 'days_cover',
        'Contract Number': 'contract_number',
    }

    date_cols = ['incur_date', 'submission_date', 'pay_date']

    dtype_bupa = {
        # 'policy_id', # This is the policy_id for future database development, f'{policy_number}__{policy_start_date:%Y%m}'
        # 'policy_number', # Contract NUmber A1
        # 'insurer', # Bupa
        # 'client_name', # Customer Name A2
        # 'policy_start_date', # Period Cell A3
        # 'policy_end_date',
        'Voucher number': str,
        'Incur Date': str,
        # 'discharge_date', convert from days cover
        'Receipt date': str,
        'Pay date': str,
        # 'claim_status',
        # 'claim_remark',
        # 'cert_true_copy',
        'Claimant': str,
        'Sex': str,
        'Age': int,
        'Member type': str,
        'Class ID': str,
        'Member Status': str,
        'Benefit Type': str,
        'Benefit': str,
        'Diagnosis description': str,
        'Chronic flag': str,
        # 'currency',
        'Presented': float,
        'Adjusted': float,
        'Network Category': str,
        # 'suboffice', # deduced from contract number columns last 2 digits
        # 'region',

        'Days Cover': int,
        'Contract Number': str,
    }


    df_ = pd.read_excel(raw_claim_path)
    client_ = df_.columns[0].split('   ')[-1]
    policy_no_ = str(df_.iloc[0,0].split('   ')[-1])

    if policy_start_date != None:
      start_d_ = pd.to_datetime(policy_start_date, format='%Y%m%d')
    else:
      start_d_ = pd.to_datetime(df_.iloc[1, 0].split(': ')[1].split(' ')[-5], format='%Y-%m-%d')
    end_d_ = pd.to_datetime(df_.iloc[1, 0].split(': ')[1].split(' ')[-1], format='%Y-%m-%d')
    df_ = pd.read_excel(raw_claim_path, skiprows=7, dtype=dtype_bupa)
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
    df_['discharge_date'].loc[df_.benefit_type == 'H'] = df_['discharge_date'] + pd.to_timedelta(df_.days_cover, unit='D')
    df_['suboffice'] = df_['contract_number'].str[-2:]

    df_[['claim_status', 'claim_remark', 'cert_true_copy', 'currency']] = np.nan
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

    df_ = df_[self.col_setup]



    return df_

  def __aia_raw_claim(self, raw_claim_path, password=None, policy_start_date=None, client_name=None, region='HK'):

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
        'Admission Date': str,
        'Incurred Date': str,
        'Discharge date': str,
        'File Date': str,
        'Received Date': str,
        'Claim Payment Date': str,
        'Process Date': str,
        # 'claim_status',
        'Remark1': str,
        # 'cert_true_copy',
        'Member ID': str,
        'Claimant No.': str,
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
        # 'chornic',
        'Billed Currency': str,
        'Submitted Claim Amount': float,
        'Presented Amount': float,
        'Paid Claim': float,
        'Reimbursed Amount': float,
        'Provider Type': str,
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
        'Admission Date': 'incur_date',
        'Incurred Date': 'incur_date',
        # 'Discharge date': 'discharge_date',
        # 'File Date': 'submission_date',
        'Received Date': 'submission_date',
        'Claim Payment Date': 'pay_date',
        'Process Date': 'pay_date',
        # 'claim_status',
        'Remark1': 'claim_remark',
        # 'cert_true_copy',
        'Member ID': 'claimant',
        'Claimant No.': 'claimant',
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
        # 'chornic',
        'Billed Currency': 'currency',
        'Submitted Claim Amount': 'incurred_amount',
        'Presented Amount': 'incurred_amount',
        'Paid Claim': 'paid_amount',
        'Reimbursed Amount': 'paid_amount',
        'Provider Type': 'panel',
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
        t_df[col] = pd.to_datetime(t_df[col], format='ISO8601')
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

    t_df.panel.fillna('Non-Panel', inplace=True)
    t_df.suboffice.fillna('00', inplace=True)


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
      'chornic': str,
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
        t_df[col].loc[~(t_df[col].isna())] = pd.to_datetime(t_df[col].loc[~(t_df[col].isna())], format='ISO8601')

    return t_df


  def add_raw_data(self, raw_claim_path, insurer=None, password=None, policy_start_date=None, client_name=None, region='HK'):

    if insurer == 'AXA':
      temp_df = self.__axa_raw_claim(raw_claim_path, password, policy_start_date, client_name, region)
    elif insurer == 'AIA':
      temp_df = self.__aia_raw_claim(raw_claim_path, password, policy_start_date, client_name, region)
    elif insurer == 'Bupa':
      temp_df = self.__bupa_raw_claim(raw_claim_path, password, policy_start_date, client_name, region)
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
    shortfall_panel = shortfall_processed_df.loc[shortfall_processed_df['panel'] == 'Panel']
    for n00 in np.arange(len(shortfall_panel)):
      __policy_id = shortfall_panel['policy_id'].iloc[n00]
      __class = shortfall_panel['class'].iloc[n00]
      __benefit = shortfall_panel['benefit'].iloc[n00]
      t_sf_df = shortfall_panel.iloc[n00, :]
      # t_sf_df['no_of_claims'] = t_sf_df['no_of_claims'] - self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) &
      #                                                                              (self.df['class'] == __class) &
      #                                                                              (self.df['benefit'] == __benefit) &
      #                                                                              (self.df['panel'] == 'Panel')].dropna().count()
      # t_sf_df['incurred_amount'] = t_sf_df['incurred_amount'] - self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) &
      #                                                                              (self.df['class'] == __class) &
      #                                                                              (self.df['benefit'] == __benefit) &
      #                                                                              (self.df['panel'] == 'Panel')].dropna().sum()
      # t_sf_df['paid_amount'] = t_sf_df['paid_amount'] - self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) &
      #                                                                        (self.df['class'] == __class) &
      #                                                                        (self.df['benefit'] == __benefit) &
      #                                                                        (self.df['panel'] == 'Panel')].dropna().sum()
      

      __no_of_claims = shortfall_panel['no_of_claims'].iloc[n00] - self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) &
                                                                                   (self.df['class'] == __class) &
                                                                                   (self.df['benefit'] == __benefit) &
                                                                                   (self.df['panel'] == 'Panel')].dropna().count()
      if __no_of_claims > 0:
        __incurred_amount = shortfall_panel['incurred_amount'].iloc[n00] - self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) &
                                                                                    (self.df['class'] == __class) &
                                                                                    (self.df['benefit'] == __benefit) &
                                                                                    (self.df['panel'] == 'Panel')].dropna().sum()
        __paid_amount = shortfall_panel['paid_amount'].iloc[n00] - self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) &
                                                                              (self.df['class'] == __class) &
                                                                              (self.df['benefit'] == __benefit) &
                                                                              (self.df['panel'] == 'Panel')].dropna().sum()
        

        t_incur_per_claim = __incurred_amount / __no_of_claims
        t_paid_per_claim = __paid_amount / __no_of_claims

        self.df['incurred_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel') & (self.df['incurred_amount'] == '')] = t_incur_per_claim
        self.df['paid_amount'].loc[(self.df['policy_id'] == __policy_id) & (self.df['class'] == __class) & (self.df['benefit'] == __benefit) & (self.df['panel'] == 'Panel') & (self.df['paid_amount_amount'] == '')] = t_paid_per_claim
    
    return



  def preprocessing(self, policy_id=None, rejected_claim=True, aso=True, smm=True):
    if aso == True:
      self.df = self.df.loc[self.df.benefit_type != 'ASO']

    if rejected_claim == True:
      reject_claim_words = ['submit', 'resumit', 'submission', 'receipt', 'signature', 'photo', 'provide', 'form']
      self.df.claim_remark.fillna('no_remark', inplace=True)
      self.df.claim_status.loc[self.df.claim_remark.str.contains('|'.join(reject_claim_words), case=False) & (self.df.paid_amount == 0)] = 'R'
      self.df = self.df.loc[self.df.claim_status != 'R']

    if smm == True:
      self.df.benefit.fillna('no_benefit', inplace=True)
      self.df.incurred_amount.loc[self.df.benefit == 'Top-up/SMM'] = 0
      self.df.incurred_amount.loc[self.df.benefit.str.contains('secondary claim', case=False)] = self.df.paid_amount.loc[self.df.benefit.str.contains('secondary claim', case=False)]
      self.df.incurred_amount.loc[self.df.benefit.str.contains('daily cash benefit', case=False)] = self.df.paid_amount.loc[self.df.benefit.str.contains('daily cash benefit', case=False)]

    return None

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
      p23_ip_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p23_ip_benefit_df['usage_ratio'] = p23_ip_benefit_df['paid_amount'] / p23_ip_benefit_df['incurred_amount']
      p23_ip_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      p23_ip_benefit_df['no_of_claims'] = p23_ip_no_claims['no_of_claims']
      p23_ip_benefit_df = p23_ip_benefit_df.unstack().stack(dropna=False)
      p23_ip_benefit_df = p23_ip_benefit_df.reindex(self.ip_order, level='benefit')
      self.p23_ip_benefit = p23_ip_benefit_df
      self.p23 = p23_ip_benefit_df
    return p23_ip_benefit_df

  def mcr_p23a_class_ip_benefit(self, by=None):
    self.ip_order = self.benefit_index.gum_benefit.loc[(self.benefit_index.gum_benefit_type == 'INPATIENT BENEFITS /HOSPITALIZATION') | (self.benefit_index.gum_benefit_type == 'MATERNITY')].drop_duplicates(keep='last').values.tolist()
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p23_ip_class_benefit_df = self.df[['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).sum()
      p23_ip_class_benefit_df['usage_ratio'] = p23_ip_class_benefit_df['paid_amount'] / p23_ip_class_benefit_df['incurred_amount']
      p23_ip_class_no_claims = self.df[['policy_number', 'year', 'class', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Hospital'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      p23_ip_class_benefit_df['no_of_claims'] = p23_ip_class_no_claims['no_of_claims']
      p23_ip_class_benefit_df = p23_ip_class_benefit_df.unstack().stack(dropna=False)
      p23_ip_class_benefit_df = p23_ip_class_benefit_df.reindex(self.ip_order, level='benefit')
      self.p23a_ip_class_benefit = p23_ip_class_benefit_df
      self.p23a = p23_ip_class_benefit_df
    return p23_ip_class_benefit_df

  def mcr_p24_op_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p24_op_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p24_op_benefit_df['usage_ratio'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['incurred_amount']
      p24_op_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      p24_op_benefit_df['no_of_claims'] = p24_op_no_claims['no_of_claims']
      p24_op_benefit_df['incurred_per_claim'] = p24_op_benefit_df['incurred_amount'] / p24_op_benefit_df['no_of_claims']
      p24_op_benefit_df['paid_per_claim'] = p24_op_benefit_df['paid_amount'] / p24_op_benefit_df['no_of_claims']
      # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
      p24_op_benefit_df = p24_op_benefit_df.unstack().stack(dropna=False)
      p24_op_benefit_df.sort_values(by=['policy_number', 'year', 'paid_amount'], ascending=[True, True, False], inplace=True)
      self.p24_op_benefit = p24_op_benefit_df
      self.p24 = p24_op_benefit_df
    return p24_op_benefit_df

  def mcr_p24a_op_class_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p24_op_class_benefit_df = self.df[['policy_number', 'year', 'class', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).sum()
      p24_op_class_benefit_df['usage_ratio'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['incurred_amount']
      p24_op_class_no_claims = self.df[['policy_number', 'year', 'class', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'class', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      p24_op_class_benefit_df['no_of_claims'] = p24_op_class_no_claims['no_of_claims']
      p24_op_class_benefit_df['incurred_per_claim'] = p24_op_class_benefit_df['incurred_amount'] / p24_op_class_benefit_df['no_of_claims']
      p24_op_class_benefit_df['paid_per_claim'] = p24_op_class_benefit_df['paid_amount'] / p24_op_class_benefit_df['no_of_claims']
      # p24_op_benefit_df.sort_values(by='paid_amount', ascending=False, inplace=True)
      p24_op_class_benefit_df = p24_op_class_benefit_df.unstack().stack(dropna=False)
      p24_op_class_benefit_df.sort_values(by=['policy_number', 'year', 'class', 'paid_amount'], ascending=[True, True, True, False], inplace=True)
      self.p24a_op_class_benefit = p24_op_class_benefit_df
      self.p24a = p24_op_class_benefit_df
    return p24_op_class_benefit_df

  def mcr_p24_dent_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p24_dent_benefit_df = self.df[['policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Dental'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).sum()
      p24_dent_benefit_df['usage_ratio'] = p24_dent_benefit_df['paid_amount'] / p24_dent_benefit_df['incurred_amount']
      p24_dent_no_claims = self.df[['policy_number', 'year', 'benefit', 'incur_date']].loc[self.df['benefit_type'] == 'Dental'].groupby(by=['policy_number', 'year', 'benefit'], dropna=False).count().rename(columns={'incur_date': 'no_of_claims'})
      p24_dent_benefit_df['no_of_claims'] = p24_dent_no_claims['no_of_claims']
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
      p26_op_panel_df = self.df[['policy_number', 'year', 'panel', 'benefit', 'incurred_amount', 'paid_amount']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'panel', 'benefit'], dropna=False).sum()
      p26_op_panel_df['usage_ratio'] = p26_op_panel_df['paid_amount'] / p26_op_panel_df['incurred_amount']
      p26_op_no_claims = self.df[['policy_number', 'year', 'panel', 'benefit', 'paid_amount']].loc[self.df['benefit_type'] == 'Clinic'].groupby(by=['policy_number', 'year', 'panel', 'benefit'], dropna=False).count().rename(columns={'paid_amount': 'no_of_claims'})
      p26_op_panel_df['no_of_claims'] = p26_op_no_claims['no_of_claims']
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
      p27_df = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incurred_amount', 'paid_amount']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).sum()
      p27_df['usage_ratio'] = p27_df['paid_amount'] / p27_df['incurred_amount']
      # if bupa == False:
      #   p27_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'paid_amount']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'paid_amount': 'no_of_claims'})
      #   p27_df['no_of_claims'] = p27_df_claims['no_of_claims']
      # else:
      p27_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incur_date']].loc[(self.df['benefit_type'] == 'Clinic') & (self.df['benefit'].str.contains('DX|PM', case=True) == False)].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'incur_date': 'no_of_claims'})
      p27_df['no_of_claims'] = p27_df_claims['no_of_claims']
      p27_df['incurred_per_claim'] = p27_df['incurred_amount'] / p27_df['no_of_claims']
      p27_df['paid_per_claim'] = p27_df['paid_amount'] / p27_df['no_of_claims']
      p27_df = p27_df.unstack().stack(dropna=False)
      p27_df.sort_values(by=['policy_number', 'year', 'class', 'dep_type', 'paid_amount'], ascending=[True, True, True, True, False], inplace=True)
      self.p27 = p27_df
    return p27_df
  
  def mcr_p28_class_dep_ip_benefit(self, by=None):
    if by == None:
      self.df['year'] = self.df.policy_start_date.dt.year
      p28_df = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incurred_amount', 'paid_amount']].loc[(self.df['benefit_type'] == 'Hospital')].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).sum()
      p28_df['usage_ratio'] = p28_df['paid_amount'] / p28_df['incurred_amount']
      # if bupa == False:
      #   p28_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'paid_amount']].loc[(self.df['benefit_type'] == 'Hospital')].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'paid_amount': 'no_of_claims'})
      #   p28_df['no_of_claims'] = p28_df_claims['no_of_claims']
      # else:
      p28_df_claims = self.df[['policy_number', 'year', 'class', 'dep_type', 'benefit', 'incur_date']].loc[(self.df['benefit_type'] == 'Hospital')].groupby(by=['policy_number', 'year', 'class', 'dep_type', 'benefit']).count().rename(columns={'incur_date': 'no_of_claims'})
      p28_df['no_of_claims'] = p28_df_claims['no_of_claims']
      p28_df['incurred_per_claim'] = p28_df['incurred_amount'] / p28_df['no_of_claims']
      p28_df['paid_per_claim'] = p28_df['paid_amount'] / p28_df['no_of_claims']
      p28_df = p28_df.unstack().stack(dropna=False)
      p28_df.sort_values(by=['policy_number', 'year', 'class', 'dep_type', 'paid_amount'], ascending=[True, True, True, True, False], inplace=True)
      self.p28 = p28_df
    return p28_df



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
    self.mcr_p27_class_dep_op_benefit(by)
    self.mcr_p28_class_dep_ip_benefit(by)

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
        self.p27.to_excel(writer, sheet_name='P.27_Class_Dep_OP_Benefit', index=True, merge_cells=False)
        self.p28.to_excel(writer, sheet_name='P.28_Class_Dep_IP_Benefit', index=True, merge_cells=False)
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

  def frequent_claimant_analysis(self, sub_policy=None):
    freq_op_list = ['General Consultation (GP)', 'Specialist Consultation (SP)', 'Chinese Med (CMT)', 'Chiro (CT)', 'Physio (PT)']
    self.df['year'] = self.df.policy_start_date.dt.year
    self.df.claimant = self.df.policy_id.str.cat(self.df.claimant, sep='_')
    __dep = self.df
    # __dep.claimant = __dep.policy_number.str.cat(__dep.year.astype(str), sep='_').str.cat(__dep.claimant, sep='_')
    __dep = __dep[['claimant', 'dep_type', 'class']]
    __dep.drop_duplicates(subset=['claimant', 'dep_type', 'class'], keep='first', inplace=True)
    __freq_df = self.df[['policy_number', 'year', 'claimant', 'benefit', 'incur_date']].dropna()
    __freq_df = __freq_df.loc[__freq_df.benefit.isin(freq_op_list)].groupby(['policy_number', 'year', 'claimant', 'benefit']).count()
    # print(__freq_df)
    __freq_df = __freq_df.unstack()
    __dep.index = __dep['claimant'].values.tolist()
    __freq_df.columns = __freq_df.columns.droplevel()
    __freq_df['total_claims'] = __freq_df.sum(axis=1)
    # __freq_df = __freq_df.sort_values(by=['policy_number', 'year', 'total_claims'], ascending=False)
    # print(__dep)
    # print(__freq_df)
    #__freq_df = pd.merge(left=__freq_df, right=__dep, left_index=True, right_index=True, how='left')
    __freq_df = pd.merge(left=__freq_df, right=__dep, left_on='claimant', right_index=True, how='left').drop(columns=['claimant'])
    __freq_df = __freq_df.sort_values(by=['policy_number', 'year', 'total_claims'], ascending=[True, True, False])
    self.frequent_analysis = __freq_df

    return __freq_df

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
      title='Number of visit of Outpatient Benefit',
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
      title='Relationship of age and incurred amount of inpatient',
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


