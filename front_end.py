
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
from Shortfall import *
from st_aggrid import AgGrid, GridUpdateMode, GridOptionsBuilder

from pygwalker.api.streamlit import StreamlitRenderer

st.set_page_config(layout='wide')

if 'raw_claim' not in st.session_state:
  st.session_state.raw_claim = False
if 'shortfall' not in st.session_state:
  st.session_state.shortfall = False
  
function_col1, function_col2, function_col3 = st.columns([1,1,1])

with function_col1:  
  if st.button('Reset'):
    if st.session_state.raw_claim == True:
      st.cache_resource.clear()
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.shortfall= False
    st.session_state.shortfall_process = False
    st.session_state.op_time = False
    st.session_state.op_policy = False
    st.session_state.dep_type_paid = False
    st.sessnion_state.dep_type_paid_class = False
    st.session_state.pygwalker = False

with function_col2:
  if st.button('Raw Claim Data'):
    st.session_state.raw_claim = True
    st.session_state.shortfall = False
    st.session_state.shortfall_process = False

with function_col3:
  if st.button('Shortfall'):
    st.session_state.shortfall = True
    st.session_state.raw_claim = False
    st.session_state.raw_process = False


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
        __d = uploaded_file.name.split('(')[-1].split("-")[0]
        __d = "".join([__d[-4:], __d[0:2], __d[2:4]])
        policy_sd_l.append(__d)
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
  file_config['Password'].loc[file_config['Insurer'] != 'AXA'] = None

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
  # st.write('### updated data')
  # st.dataframe(new_file_config)
  st.write('---')
  st.header('Bupa Shortfall')
  st.write("""
  The shortfall summary will be used for supplementing the masked data of Panel Doctor Visits. 
  
  The shortfall summary should contains both Non-Panel and Overall breakdown.
  """)

  upload_raw_shortfall_l = []
  uploaded_raw_shortfall_file_list = []
  full_raw_shortfall_file_list = []

  uploaded_raw_shortfall_files = st.file_uploader("Upload bupa shortfall excel files", accept_multiple_files=True)
  for uploaded_raw_shortfall_file in uploaded_raw_shortfall_files:
      # st.write("filename:", uploaded_file.name)
      upload_raw_shortfall_l.append(uploaded_raw_shortfall_file.name)
      uploaded_raw_shortfall_file_list.append(uploaded_raw_shortfall_file)

      import tempfile
      import os
      temp_dir = tempfile.mkdtemp()
      path = os.path.join(temp_dir, uploaded_raw_shortfall_file.name)
      full_raw_shortfall_file_list.append(path)
      with open(path, "wb") as f:
            f.write(uploaded_raw_shortfall_file.getvalue())

  
  shortfall_files = pd.DataFrame(upload_raw_shortfall_l, columns=['File Name'])

  st.write('---')
  mcr_by_col1, mcr_by_col2, mcr_by_col3, mcr_by_col4, mcr_by_col5, mcr_by_col6 = st.columns([1,1,1,1,1,1])
  with mcr_by_col1:
    dep_toggle = st.toggle('MCR by dependent type')
  with mcr_by_col2:
    suboffice_toggle = st.toggle('MCR by suboffice')
  with mcr_by_col3:
    gender_toggle = st.toggle('MCR by gender')
  with mcr_by_col4:
    diagnosis_toggle = st.toggle('MCR by diagnosis')
  

  by = []
  if dep_toggle:
    by.append('dep_type')
  if suboffice_toggle:
    by.append('suboffice')
  if gender_toggle:
    by.append('gender')
  if diagnosis_toggle:
    by.append('diagnosis')
  if len(by) == 0:
    by=None

  if 'raw_process' not in st.session_state:
    st.session_state.raw_process = False

  if st.button('File Configuration Confirm, Proceed!'):
    st.session_state.raw_process = True

  if st.session_state.raw_process == True:
    file_config = new_file_config
    st.write('---')
    st.header('MCR Data Download')
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

    if len(upload_raw_shortfall_l) > 0:
      sf_ = Shortfall()
      for n0 in range(len(shortfall_files)):
        sf_.add_shortfall(full_raw_shortfall_file_list[n0])

      raw_.bupa_shortfall_supplement(sf_.df)

    
    __freq = raw_.frequent_claimant_analysis()





    # Write files to in-memory strings using BytesIO
    # See: https://xlsxwriter.readthedocs.io/workbook.html?highlight=BytesIO#constructor

    data_download_col1, data_download_col2, data_download_col3 = st.columns([1,1,1]) 

    with data_download_col1:
      st.download_button('MCR data', 
                        data=raw_.mcr_pages(export=True, by=by),
                        file_name="mcr.xlsx",
                        mime="application/vnd.ms-excel")
    
    with data_download_col2:
      st.download_button('Raw Claim Consolidated', 
                        data=raw_.export_database(),
                        file_name="raw_claim_data.csv",
                        mime="application/vnd.ms-excel")

    with data_download_col3:  
      st.download_button('Frequent Claimant Analysis', 
                        data=__freq.to_csv(index=True).encode('utf-8'),
                        file_name="freq_claimant.csv",
                        mime="application/vnd.ms-excel")

    st.write('---')
    st.header('Outpatient Benefit Charts')

    chart_col1, chart_col2, chart_col3 = st.columns([1,1, 1]) 
    if 'op_time' not in st.session_state:
      st.session_state.op_time = False
    if 'op_policy' not in st.session_state:
      st.session_state.op_policy = False
    if 'age_class_scatter' not in st.session_state:
      st.session_state.age_class_scatter = False


    with chart_col1:
      if st.button('Outpatient Benefit Time Series'):
        st.session_state.op_time = True
        st.session_state.op_policy = False
        st.session_state.age_class_scatter = False
    with chart_col2:
      if st.button('Outpatient Benefit by Policy Id'):
        st.session_state.op_time = False
        st.session_state.op_policy = True
        st.session_state.age_class_scatter = False
    with chart_col3:
      if st.button('Class Age'):
        st.session_state.op_time = False
        st.session_state.op_policy = False
        st.session_state.age_class_scatter = True

    if st.session_state.op_time == True:
      fig = raw_.benefit_op_monthly()
      st.plotly_chart(fig, use_container_width=False)
    if st.session_state.op_policy == True:
      fig = raw_.benefit_op_yearly_bar()
      st.plotly_chart(fig, use_container_width=False)
    if st.session_state.age_class_scatter == True:
      fig = raw_.class_age_scatter()
      st.plotly_chart(fig, use_container_width=False)

    st.write('---')
    st.header('Dependent Type Charts')
    dep_type_chart_col1, dep_type_chart_col2, dep_type_chart_col3, dep_type_chart_col4, dep_type_chart_col5, dep_type_chart_col6  = st.columns([1 ,1 ,1,1,1,1]) 
    if 'dep_type_paid' not in st.session_state:
      st.session_state.dep_type_paid = False

    with dep_type_chart_col1:
      if st.button('Paid by Dependent Policy Id'):
        st.sessnion_state.dep_type_paid = True
        st.sessnion_state.dep_type_paid_class = False

    with dep_type_chart_col2:
      if st.button('Paid Amount by Dependent Class Policy Id'):
        st.session_state.dep_type_paid = False
        st.sessnion_state.dep_type_paid_class = True

    if st.session_state.dep_type_paid == True:
      fig = raw_.paid_amount_by_dep_type__policy_year()
      st.plotly_chart(fig, use_container_width=False)
    if st.session_state.dep_type_paid_class == True:
      fig = raw_.paid_amount_by_dep_type_class_policy_year()
      st.plotly_chart(fig, use_container_width=False)

    

    st.write('---')
    st.header('Interactive Dashboard')
    if 'pygwalker' not in st.session_state:
      st.session_state.pygwalker = False

    if st.button('PyGWalker Interactive Dashboard'):
        st.session_state.pygwalker = True

    if st.session_state.pygwalker == True:
      interactive_df = raw_.df
      pyg_app = StreamlitRenderer(interactive_df)
      pyg_app.explorer()






if st.session_state.shortfall == True:
  st.write("""
  # Gain Miles Assurance Consultancy Ltd

  ### Bupa Shortfall MCR data calculation tool
  """)
  st.write("---")

  upload_file_l = []
  insurer_l = []
  password_l = []
  policy_sd_l = []
  
  uploaded_file_list = []
  full_file_list = []

  uploaded_files = st.file_uploader("Upload bupa shortfall excel files", accept_multiple_files=True)
  for uploaded_file in uploaded_files:
      # st.write("filename:", uploaded_file.name)
      upload_file_l.append(uploaded_file.name)
      uploaded_file_list.append(uploaded_file)

      import tempfile
      import os
      temp_dir = tempfile.mkdtemp()
      path = os.path.join(temp_dir, uploaded_file.name)
      full_file_list.append(path)
      with open(path, "wb") as f:
            f.write(uploaded_file.getvalue())

  
  shortfall_files = pd.DataFrame(upload_file_l, columns=['File Name'])

  # st.write('### Uploaded Files')
  # st.dataframe(shortfall_files)

  if 'shortfall_process' not in st.session_state:
    st.session_state.shortfall_process = False

  if st.button('File Upload Confirm, Proceed!'):
    st.session_state.shortfall_process = True

  if st.session_state.shortfall_process == True:
    # shortfall_files
    st.write('---')
    # ben_fp = 'benefit_indexing.xlsx'
    sf_ = Shortfall()
    for n0 in range(len(shortfall_files)):
      sf_.add_shortfall(full_file_list[n0])
    
    sf_.remove_overall()
    data_download_col1, data_download_col2= st.columns([1,1]) 

    with data_download_col1:
      st.download_button('MCR data', 
                        data=sf_.mcr_pages(export=True),
                        file_name="mcr.xlsx",
                        mime="application/vnd.ms-excel")
    with data_download_col2:
      st.download_button('Shortfall Consolidated', 
                        data=sf_.export_database(),
                        file_name="shortfall_data.csv",
                        mime="application/vnd.ms-excel")
    
