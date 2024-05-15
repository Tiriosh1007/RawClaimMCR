
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')
sns.set(rc={'figure.figsize':(5,5)})
plt.rcParams["axes.formatter.limits"] = (-99, 99)

from RawClaimData import *
from st_aggrid import AgGrid, GridUpdateMode, GridOptionsBuilder

if 'raw_claim' not in st.session_state:
  st.session_state.raw_claim = False
  
if st.button('Reset'):
  if st.session_state.raw_claim == True:
    st.cache_resource.clear()
  st.session_state.raw_claim = False
  st.session_state.process = False

if st.button('Raw Claim Data'):
  st.session_state.raw_claim = True


if st.session_state.raw_claim == True:
  st.write("""
  # Gain Miles Assurance Consultancy Ltd

  ### Raw Claim Data Converter and MCR data calculation tool
  """)
  st.write("---")

  upload_file_l = []
  insurer_l = []
  password_l = []
  policy_sd_l = []
  
  uploaded_file_list = []
  full_file_list = []

  uploaded_files = st.file_uploader("Upload raw claim excel .xlsx files", accept_multiple_files=True)
  for uploaded_file in uploaded_files:
      # st.write("filename:", uploaded_file.name)
      upload_file_l.append(uploaded_file.name)
      uploaded_file_list.append(uploaded_file)
      if 'EB' in uploaded_file.name:
        insurer_l.append('AXA')
        policy_sd_l.append(uploaded_file.name.split('_')[3])
        password_l.append("".join(['axa', str(dt.date.today().year)]))
      elif 'HSD' in uploaded_file.name or 'GMD' in uploaded_file.name:
        insurer_l.append('AIA')
        policy_sd_l.append(uploaded_file.name.split('(')[-1].split("-")[0])
        password_l.append("")
      elif 'Claims Raw' in uploaded_file.name:
        insurer_l.append('Bupa')
        policy_sd_l.append("".join([uploaded_file.name.split('-')[0].split(' ')[-1],'01']))
        password_l.append("")

      import tempfile
      import os
      temp_dir = tempfile.mkdtemp()
      path = os.path.join(temp_dir, uploaded_file.name)
      full_file_list.append(path)
      with open(path, "wb") as f:
            f.write(uploaded_file.getvalue())

  file_config = pd.DataFrame(upload_file_l, columns=['File Name'])
  insurer_df = pd.DataFrame(insurer_l, columns=['Insurer'])
  password_df = pd.DataFrame(password_l, columns=['Password'])
  policy_sd_df = pd.DataFrame(policy_sd_l, columns=['Policy start date'])

  file_config = pd.concat([file_config, insurer_df, password_df, policy_sd_df], axis=1, ignore_index=False)
  # file_config['Password'].loc[file_config['Insurer'] != 'AXA'] = None
  #if len(upload_file_l > 1):
    # insurer_df
    #file_config = pd.DataFrame([upload_file_l, insurer_l, password_l, policy_sd_l], columns=['File Name', 'Insurer', 'Password', 'Policy start date'])
  # file_config['Insurer'] = 'AIA/AXA/Bupa'
  # file_config['Insurer'].loc[file_config['File Name'].str.contains('EB', case=True)] = 'AXA'
  # file_config['Insurer'].loc[file_config['File Name'].str.contains('HSD|GMD', case=True)] = 'AIA'
  # file_config['Insurer'].loc[file_config['File Name'].str.contains('Claims Raw', case=True)] = 'Bupa'
  # file_config['Password'] = None
  # file_config['Policy start date'] = None
  # file_config['Policy start date'].loc[file_config['Insurer'] == 'AIA'] = file_config['File Name'].str.split('(')[-1].str.split("-")[0]
  # file_config['Policy start date'].loc[file_config['Insurer'] == 'AIA'] = (file_config['Policy start date'].loc[file_config['Insurer'] == 'AIA'].str[-4:] + 
  #                                                                          file_config['Policy start date'].loc[file_config['Insurer'] == 'AIA'].str[0:2] + 
  #                                                                          file_config['Policy start date'].loc[file_config['Insurer'] == 'AIA'].str[2:4])
  # file_config['Policy start date'].loc[file_config['Insurer'] == 'AXA'] = file_config['File Name'].str.split('_')[3]
  # file_config['Policy start date'].loc[file_config['Insurer'] == 'Bupa'] = file_config['File Name'].str.split('-')[0].str.split(' ')[-1] + '01'
  file_config['Client Name'] = 'Input Client Name'
  file_config['Region'] = 'HK'

  st.write('---')
  st.header('Raw Claim File Configurations')
  st.write("""
  Please input the configurations:

  1. Insurers: Bupa/ AIA/ AXA. If it is consolidated raw claim please leave blank.

  2. Password: If no password please leave blank.

  3. Policy start date: In yyyymmdd form. Ie 1 Jul 2023 => 20230701.

  4. Client Name: Free to input but good to make sure it aligns with previous used name.

  5. Region: HK/ MO.
  """)
  gb = GridOptionsBuilder.from_dataframe(file_config)
  gb.configure_column('Insurer', editable=True)
  gb.configure_column('Password', editable=True)
  gb.configure_column('Policy start date', editable=True)
  gb.configure_column('Client Name', editable=True)
  gb.configure_column('Region', editable=True)

  ag = AgGrid(file_config,
              gridOptions=gb.build(),
              update_mode=GridUpdateMode.VALUE_CHANGED,
              height=350,
              weight=1200,
              allow_insafe_jscode=True,
              enable_enterprise_modules=False)

  new_file_config = ag['data']
  st.write('#### updated data')
  st.dataframe(new_file_config)

  if 'process' not in st.session_state:
    st.session_state.process = False

  if st.button('File Configuration Confirm, Proceed!'):
    st.session_state.process = True

  if st.session_state.process == True:
    file_config = new_file_config
    st.write('---')
    ben_fp = 'benefit_indexing.xlsx'
    raw_ = RawClaimData(ben_fp)
    for n0 in range(len(file_config)):
      raw_.add_raw_data(full_file_list[n0],
                        insurer=file_config['Insurer'].iloc[n0], 
                        password=file_config['Password'].iloc[n0], 
                        policy_start_date=file_config['Policy start date'].iloc[n0], 
                        client_name=file_config['Client Name'].iloc[n0], 
                        region=file_config['Region'].iloc[n0])

    raw_.preprocessing()
    




    # Write files to in-memory strings using BytesIO
    # See: https://xlsxwriter.readthedocs.io/workbook.html?highlight=BytesIO#constructor
  

    st.download_button('MCR data', 
                      data=raw_.mcr_pages(export=True),
                      file_name="mcr.xlsx",
                      mime="application/vnd.ms-excel")
    st.download_button('Raw Claim Consolidated', 
                      data=raw_.export_database(),
                      file_name="raw_claim_date.csv",
                      mime="application/vnd.ms-excel")





