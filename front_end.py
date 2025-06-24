
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from datetime import timedelta
import warnings
# import ollama
# from openai import OpenAI
import requests, json
from pathlib import Path

open_rounter_api_key = st.secrets['api_key']
prompt_lib_xml = 'prompt_lib.xml'
prompt_lib = pd.read_xml('prompt_lib.xml')
prompt = ""
import base64
import json
import io
from streamlit_pdf_viewer import pdf_viewer
warnings.filterwarnings('ignore')
sns.set(rc={'figure.figsize':(5,5)})
plt.rcParams["axes.formatter.limits"] = (-99, 99)

from RawClaimData import *
from Shortfall import *
from ColumnNameManagement import *
from MemberCensus import *
from MCRConvert import *
from st_aggrid import AgGrid, GridUpdateMode, GridOptionsBuilder

from pygwalker.api.streamlit import StreamlitRenderer

st.set_page_config(layout='wide')

if 'raw_claim' not in st.session_state:
  st.session_state.raw_claim = False
if 'shortfall' not in st.session_state:
  st.session_state.shortfall = False
if 'col_management' not in st.session_state:
  st.session_state.col_management = False
if 'member_census' not in st.session_state:
  st.session_state.member_census = False
if 'mcr_convert' not in st.session_state:
  st.session_state.mcr_convert = False  
if 'ocr' not in st.session_state:
  st.session_state.ocr = False

  
function_col1, function_col2, function_col3, function_col4, function_col5 , function_col6, function_col7, function_col8= st.columns([1,1,1, 1, 1,1,1,1])

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
    st.session_state.dep_type_paid_class = False
    st.session_state.pygwalker = False
    st.session_state.mcr_data = False
    st.session_state.col_management = False
    st.session_state.member_census = False
    st.session_state.member_census_proceed = False
    st.session_state.mcr_convert = False
    st.session_state.mcr_convert_uploaded = False
    st.session_state.ocr = False
    st.session_state.ocr_result = False

with function_col2:
  if st.button('Raw Claim Data'):
    st.session_state.raw_claim = True
    st.session_state.shortfall = False
    st.session_state.shortfall_process = False
    st.session_state.col_management = False
    st.session_state.member_census = False
    st.session_state.mcr_convert = False
    st.session_state.ocr = False

with function_col3:
  if st.button('Shortfall'):
    st.session_state.shortfall = True
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.col_management = False
    st.session_state.member_census = False
    st.session_state.mcr_convert = False
    st.session_state.ocr = False

with function_col4:
  if st.button('Column Management'):
    st.session_state.col_management = True
    st.session_state.shortfall = False
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.member_census = False
    st.session_state.mcr_convert = False
    st.session_state.ocr = False

with function_col5:
  if st.button('Member Census'):
    st.session_state.col_management = False
    st.session_state.shortfall = False
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.member_census = True
    st.session_state.mcr_convert = False
    st.session_state.ocr = False

with function_col6:
  if st.button('Convert MCR'):
    st.session_state.col_management = False
    st.session_state.shortfall = False
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.member_census = False
    st.session_state.mcr_convert = True
    st.session_state.ocr = False

with function_col7:
  if st.button('OCR'):
    st.session_state.col_management = False
    st.session_state.shortfall = False
    st.session_state.raw_claim = False
    st.session_state.raw_process = False
    st.session_state.member_census = False
    st.session_state.mcr_convert = False
    st.session_state.ocr = True


# ========================================================================================================
# Raw Claim Analysis Session
# ========================================================================================================

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
      elif 'Consolidation Report' in uploaded_file.name:
        insurer_l.append('Blue Cross')
        policy_sd_l.append(uploaded_file.name.split('_')[-1].split(' to ')[0])
        password_l.append("")
      elif 'AXA_single' in uploaded_file.name:
        insurer_l.append('AXA single')
        policy_sd_l.append("".join([uploaded_file.name.split('_')[-1].split('-')[0],'01']))
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

  1. Insurers: Bupa/ AIA/ AXA/ Blue Cross/ AXA Single. If it is consolidated raw claim please leave blank.

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
    
    # st.dataframe(file_config)

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
  mcr_filt_col1, mcr_filt_col2, mcr_filt_col3, mcr_filt_col4, mcr_filt_col5, mcr_filt_col6 = st.columns([1,1,1,1,1,1])
  with mcr_filt_col1:
    rej_claim_toggle = st.toggle('Filter Rejected Claims', value=True)
  with mcr_filt_col2:
    aso_toggle = st.toggle('Filter ASO Claims', value=True)
  with mcr_filt_col3:
    smm_toggle = st.toggle('SMM Claims Incurred to zero', value=True)
  with mcr_filt_col4:
    diagnosis_conversion_toggle = st.toggle('Diagnosis Name Conversion', value=True)
  with mcr_filt_col5:
    group_optical_toggle = st.toggle('Group Optical', value=False)
    
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
    print(rej_claim_toggle)
    raw_.preprocessing(policy_id=None, rejected_claim=rej_claim_toggle, aso=aso_toggle, smm=smm_toggle, diagnosis=diagnosis_conversion_toggle, group_optical=group_optical_toggle)

    if len(upload_raw_shortfall_l) > 0:
      sf_ = Shortfall()
      for n0 in range(len(shortfall_files)):
        sf_.add_shortfall(full_raw_shortfall_file_list[n0])

      raw_.bupa_shortfall_supplement(sf_.df)


    # Write files to in-memory strings using BytesIO
    # See: https://xlsxwriter.readthedocs.io/workbook.html?highlight=BytesIO#constructor

    if 'mcr_data' not in st.session_state:
      st.session_state.mcr_data = False

    if st.button('MCR and Raw Claim Generate'):
      st.session_state.mcr_data = True

   

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

    # with data_download_col3:  
    #   st.download_button('Frequent Claimant Analysis', 
    #                     data=raw_.frequent_claimant_analysis(),
    #                     file_name="freq_claimant.xlsx",
    #                     mime="application/vnd.ms-excel")
    st.write('---')
    st.header('Frequent Claimant Analysis')
    st.download_button('Frequent Claimant Analysis', 
                        data=raw_.frequent_claimant_analysis(),
                        file_name="freq_claimant.xlsx",
                        mime="application/vnd.ms-excel")

    st.write('---')
    st.header('Fair IBNR estimation based on historical data')
    st.write("""
            ### Please make sure that the raw claim data provides the claim submission date!
             """)
    ibnr_month = st.slider("Month for IBNR estimation:",
                           1, 12, 9)
    st.dataframe(data = raw_.fair_ibnr_estimation(months=ibnr_month))

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
    if 'dep_type_paid_class' not in st.session_state:
      st.session_state.dep_type_paid_class = False

    with dep_type_chart_col1:
      if st.button('Paid by Dependent Policy Id'):
        st.session_state.dep_type_paid = True
        st.session_state.dep_type_paid_class = False

    with dep_type_chart_col2:
      if st.button('Paid Amount by Dependent Class Policy Id'):
        st.session_state.dep_type_paid = False
        st.session_state.dep_type_paid_class = True

    if st.session_state.dep_type_paid == True:
      fig = raw_.paid_amount_by_dep_type_policy_year()
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


# ========================================================================================================
# Shortfall Convert to MCR
# ========================================================================================================



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
    

# ========================================================================================================
# Column Name Management Session (to be fixed)
# ========================================================================================================



if st.session_state.col_management == True:
  
  st.write("""
  # Gain Miles Assurance Consultancy Ltd

  """)
  st.write("---")

  st.header('Column Management Tool')

  col_manage = ColNameMgnt()
  filter_col1, filter_col2, filter_col3 = st.columns([1,1,1])

  insurer_l = col_manage.col_df['insurer'].unique()
  col_name_l = col_manage.col_df['col_name'].unique()
  with filter_col1:
    insurer_filter = st.selectbox(
    "Insurer Filter",
    options=insurer_l,
    index=None,
    placeholder="Select Insurer",
    )
  with filter_col2:
    col_filter = st.selectbox(
      "Column Filter",
      options=col_name_l,
      index=None,
      placeholder="Select Our Column Name",
    )

  for_display = col_manage.col_df.copy(deep=True)
  if insurer_filter != None:
    for_display = for_display[col_manage.col_df['insurer'] == insurer_filter]
  if col_filter != None:
    for_display = for_display[col_manage.col_df['col_name'] == col_filter]
  st.dataframe(for_display)
  st.write("---")
  st.write("Update Column Name: [link](https://docs.google.com/spreadsheets/d/18nqxO0SIYJ2d9r0_Gz1IKySDb-7KdhswpTH8K85dp-o/edit?usp=sharing)")
  if False:"""st.write('#### Add Column Name')
  df_to_add = pd.DataFrame(columns=['insurer', 'ins_col_name', 'col_name', 'data_type'])
  add_col_name_col1, add_col_name_col2, add_col_name_col3, add_col_name_col4 = st.columns([1,1,1,1])
  with add_col_name_col1:
    add_insurer = st.selectbox(
      "Insurer",
      options=insurer_l,
      index=None,
      placeholder="Insurer",
    )
  with add_col_name_col2:
    add_ins_col_name = st.text_input('Insurer Column Name')
  with add_col_name_col3:
    add_our_col_name = st.selectbox(
      "Our Column Name",
      options=col_name_l,
      index=None,
      placeholder="Our Column Name",
    )
  with add_col_name_col4:
    add_col_data_type = st.selectbox(
      "Data Type (Please select 'object' for Dates)",
      options=['object', 'int', 'float'],
      index=None,
      placeholder="Data Type",
    )
  if st.button('Add Column Name'):
    df_to_add = pd.concat([df_to_add, pd.DataFrame([[add_insurer, add_ins_col_name, add_our_col_name, add_col_data_type]], columns=['insurer', 'ins_col_name', 'col_name', 'data_type'])])
  if len(df_to_add) > 0:
    st.dataframe(df_to_add)

  st.write('---')
  st.write('#### Remove Column Name')
  remove_col_name_col1, remove_col_name_col2, remove_col_name_col3 = st.columns([1,1,1])
  df_to_remove = pd.DataFrame(columns=['insurer', 'ins_col_name', 'col_name'])
  with remove_col_name_col1:
    remove_insurer = st.selectbox(
      "Remove Insurer",
      options=insurer_l,
      index=None,
      placeholder="Insurer",
    )
  with remove_col_name_col2:
    remove_col_name = st.selectbox(
      "Remove Our Column Name",
      options=col_name_l,
      index=None,
      placeholder="Our Column Name",
    )
  remove_ins_col_l = col_manage.col_df['ins_col_name'].loc[(col_manage.col_df['insurer'] == remove_insurer) & (col_manage.col_df['col_name'] == remove_col_name)].unique()
  with remove_col_name_col3:
    remove_ins_col = st.selectbox(
      "Remove Insurer Column Name",
      options=remove_ins_col_l,
      index=None,
      placeholder="Insurer Column Name",
    )
  if st.button('Remove Column Name'):
    df_to_remove = pd.concat([df_to_remove, pd.DataFrame([[remove_insurer, remove_ins_col, remove_col_name]], columns=['insurer', 'ins_col_name', 'col_name'])])
  if len(df_to_remove) > 0:
    st.dataframe(df_to_remove)


  st.write('---')
  if st.button('Confirm Update Column Name'):
    col_manage.add_col_mapper(df_to_add)
    col_manage.remove_col_mapper(df_to_remove)
    st.write('Column Name Updated!')
    st.session_state.col_management = False"""


# ========================================================================================================
# Member Census Chart and Info Session
# ========================================================================================================

if st.session_state.member_census == True:
  st.write("""
  # Gain Miles Assurance Consultancy Ltd

  ### Member Census Analytic Tool
  """)
  st.write("---")
  upload_file_l = []
  insurer_l = []
  password_l = []
  policy_sd_l = []
  
  uploaded_file_list = []
  full_file_list = []

  uploaded_files = st.file_uploader("Upload member census excel .xlsx files", accept_multiple_files=True)
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

  file_config = pd.DataFrame(upload_file_l, columns=['File Name'])
  insurer_df = pd.DataFrame(insurer_l, columns=['Insurer'])
  password_df = pd.DataFrame(password_l, columns=['Password'])

  file_config = pd.concat([file_config, insurer_df, password_df], axis=1, ignore_index=False)

  st.write('---')
  st.header('Member Census File Configurations')
  st.write("""
  Please input the configurations:

  1. Insurers: Bupa/ AIA/ AXA/ Blue Cross.

  2. Password: If no password please leave blank.

  3. Policy start date: In yyyymmdd form. Ie 1 Jul 2023 => 20230701.

  4. Client Name: Free to input but good to make sure it aligns with previous used name.

  """)
  gb = GridOptionsBuilder.from_dataframe(file_config)
  gb.configure_column('Insurer', editable=True)
  gb.configure_column('Password', editable=True)

  ag = AgGrid(file_config,
              gridOptions=gb.build(),
              update_mode=GridUpdateMode.VALUE_CHANGED,
              height=350,
              weight=1200,
              allow_insafe_jscode=True,
              enable_enterprise_modules=False)
  
  new_file_config = ag['data']
  chart_input_col1, chart_input_col2, chart_input_col3, chart_input_col_4, chart_input_col_5, chart_input_col_6 = st.columns([1,1,1,1,1,1])
  with chart_input_col1:
    xmax = st.number_input("X-axis max value", value=1000)
  with chart_input_col2:
    xstep = st.number_input("X-axis step value", value=50)
  with chart_input_col3:
    ystep = st.number_input("Age step value", value=10)
  with chart_input_col_4:
    width = st.number_input("Graph width", value=800)
  with chart_input_col_5:
    height = st.number_input("Graph height", value=600)
  
  if st.button("Confirm"):
    st.session_state.member_census_proceed = True
    # member_files = pd.DataFrame(uploaded_file_list, columns=['File Name'])
  if st.session_state.member_census_proceed == True:
    file_config = new_file_config
    member_census = MemberCensus()
    for n0 in range(len(file_config)):
      member_census.get_member_df(full_file_list[n0],
                        insurer=file_config['Insurer'].iloc[n0], 
                        password=file_config['Password'].iloc[n0], 
                        )
      
    member_census.member_df_processing()
    member_census.get_general_info()
    st.write('---')
    st.write('### General Information')
    gen_info_col1, gen_info_col2, gen_info_col3 = st.columns([1,1,1])
    with gen_info_col1:
      st.write('Total Member:', member_census.total_member)
      st.write('Employee Average Age:', member_census.ee_age)
      st.write('Employee Mix - Male:Female', member_census.ee_mix_ratio)
      st.write('Dependent Ratio:', member_census.dep_ratio)
    with gen_info_col2:
      st.write('Dependent Mix:')
      st.dataframe(member_census.dep_ratio_df)
    with gen_info_col3:
      st.write('Employee Mix:')
      st.dataframe(member_census.ee_mix_df)
    st.write('---')
      
    member_census.set_graph_layout(xmax, xstep, ystep, width, height)
    member_census.get_gender_distribution()
    fig = member_census.butterfly_plot()
    st.plotly_chart(fig, use_container_width=False)
    st.dataframe(member_census.gender_dis_df)
    st.write('---')
    fig = member_census.butterfly_plot_dep()
    st.plotly_chart(fig, use_container_width=False)
    st.dataframe(member_census.gender_dis_dep_df)
    st.write('---')
    member_df_col_1, member_df_col_2, member_df_col_3, member_df_col_4, member_df_col_5, member_df_col_6 = st.columns([1,1,1,1,1,1])
    with member_df_col_1:
      if st.button('Age & Gender'):
        st.dataframe(member_census.gender_dis_df)
    with member_df_col_2:
      if st.button('Age & Gender & Dep'):
        st.dataframe(member_census.gender_dis_dep_df)
    with member_df_col_3:
      if st.button('Age & Class'):
        st.dataframe(member_census.dis_cls_df)
    with member_df_col_4:
      if st.button('Age & Class & Gender'):
        st.dataframe(member_census.gender_dis_cls_df)
    with member_df_col_5:
      if st.button('Class & Dep'):
        st.dataframe(member_census.cls_df)




# ========================================================================================================
# MCR Convert
# ========================================================================================================

if st.session_state.mcr_convert == True:
  if 'mcr_convert_uploaded' not in st.session_state:
    st.session_state.mcr_convert_uploaded = False
  st.write("""
           # Gain Miles Assurance Consultancy Ltd
           ### MCR  to Presentation and GMI upload Tool
           """)
  st.write("---")
  upload_file_l = []
  mcr_file_uploaded = st.file_uploader("Upload the MCR analysis excel produced by this system", accept_multiple_files=False, key='mcr_convert_uploader')

    
  import tempfile
  import os
  if mcr_file_uploaded is not None:
    temp_dir = tempfile.mkdtemp()
    path = os.path.join(temp_dir, mcr_file_uploaded.name)
    with open(path, "wb") as f:
      f.write(mcr_file_uploaded.getvalue())

  if st.button('Confirm MCR Analysis File Upload'):
    st.session_state.mcr_convert_uploaded = True
  
  if st.session_state.mcr_convert_uploaded == True:
    st.write("---")
    mcr_convert_  = MCRConvert(path)
    st.dataframe(mcr_convert_.policy_info)
    mcr_covert_policy_info_df = mcr_convert_.policy_info.copy(deep=True)
    
    st.write("""
             Please Select the policy number, year, start date, and end date for both previous year and current year.

             Please make sure that the class of both years are in sync, otherwise the conversion will not work properly.

             ### *If the class is not in sync, please just leave previous year empty.*
             """)
    mcr_convert_year_cofig_col1, mcr_convert_year_cofig_col2 = st.columns([1, 1])
    with mcr_convert_year_cofig_col1:
      st.write('Previous Year')
      
      previous_policy_num = st.selectbox('Policy Number',
                   options=mcr_covert_policy_info_df['policy_number'].unique(),
                   index=None,
                   key='prev_policy_number')
      previous_year = st.selectbox('Year',
                   options=mcr_covert_policy_info_df['year'].loc[mcr_covert_policy_info_df['policy_number'] == previous_policy_num].unique(),
                   index=None,
                   key='prev_year')
      previous_year_start_date = str(format((st.date_input('Start Date', key='prev_year_start_date', value="today")), "%-d/%-m/%Y"))
      previous_year_end_date = str(format((st.date_input('End Date', key='prev_year_end_date', value="today")), "%-d/%-m/%Y"))
      st.write(" ### Optional")
      previous_year_loss_ratio_text = st.text_area("Please input the loss ratio text obtained from the OCR session for previous year.", key='previous_year_loss_ratio_text')

    with mcr_convert_year_cofig_col2:
      st.write('Current Year')

      current_policy_num = st.selectbox('Policy Number',
                         options=mcr_covert_policy_info_df['policy_number'].unique(),
                         index=None,
                         key='current_policy_number')
      current_year = st.selectbox('Year',
                          options=mcr_covert_policy_info_df['year'].loc[mcr_covert_policy_info_df['policy_number'] == current_policy_num].unique(),
                          index=None,
                          key='current_year')
      current_year_start_date = str(format((st.date_input('Start Date', key='current_year_start_date', value="today")), "%-d/%-m/%Y"))
      current_year_end_date = str(format((st.date_input('End Date', key='current_year_end_date', value="today")), "%-d/%-m/%Y"))
      st.write(" ### Optional")
      current_year_loss_ratio_text = st.text_area("Please input the loss ratio text obtained from the OCR session for current year.", key='current_year_loss_ratio_text')

    st.write('---')
    loss_ratio_group_optical = st.toggle('Group Optical Items into Clinical', value=True)
    if st.button('Confirm Year Configuration'):
      mcr_convert_.set_policy_input(previous_policy_num, previous_year_start_date, previous_year_end_date, previous_year, 
                                    current_policy_num, current_year_start_date, current_year_end_date, current_year)
      
      mcr_convert_.loss_ratio_text_convert(previous_year_loss_ratio_text, current_year_loss_ratio_text, loss_ratio_group_optical)
      mcr_convert_.convert_all()
      st.download_button('Download MCR Converted Excel',
                          mcr_convert_.save(),
                          file_name=f'GMI_{current_policy_num}_{current_year}.xlsx',
                          mime="application/vnd.ms-excel")


      

      





# ========================================================================================================
# OCR Session
# ========================================================================================================


if st.session_state.ocr == True:
  # Title and description in main area
  st.markdown("""
      # <img src="data:image/png;base64,{}" width="50" style="vertical-align: -12px;"> Gemini 2.5 Flash Preview ver.0520 OCR
      """.format(base64.b64encode(open("./asset/gemma3.png", "rb").read()).decode()), unsafe_allow_html=True)
  
  def encode_pdf_to_base64(pdf_path):
    with open(pdf_path, "rb") as pdf_file:
        return base64.b64encode(pdf_file.read()).decode('utf-8')
  

  url = "https://openrouter.ai/api/v1/chat/completions"
  headers = {
    "Authorization": f"Bearer {open_rounter_api_key}",
    "Content-Type": "application/json"
  }
  


  # Add clear button to top right
  col1, col2 = st.columns([6,1])
  with col2:
    if st.button("Clear 🗑️"):
      if 'ocr_result' in st.session_state:
        del st.session_state['ocr_result']
        st.rerun()

  st.markdown('<p style="margin-top: -20px;">Extract structured text from pdf using Gemini 2.5 Flash Preview Vision!</p>', unsafe_allow_html=True)
  st.markdown("---")

  prompt_lib_col1, prompt_lib_col2, prompt_lib_col3, prompt_lib_col4, prompt_lib_col5, prompt_lib_col6,  = st.columns([1, 1, 1, 1, 1, 1])

  with prompt_lib_col1:
    if st.button("Loss Ratio"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "Loss Ratio"].to_dict("records")[0]

  with prompt_lib_col2:
    if st.button("AIA 105 Overall Usage"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "AIA 105 Overall Usage"].to_dict("records")[0]
    if st.button("AIA 101 Hosp Usage"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "AIA 101 Hosp Usage"].to_dict("records")[0]
    if st.button("AIA 102 Clinical Usage"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "AIA 102 Clinical Usage"].to_dict("records")[0]
  
  with prompt_lib_col3:
    if st.button("Bupa Shortfall Non-healthnet"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "Bupa Shortfall Non-healthnet"].to_dict("records")[0]
    if st.button("Bupa Shortfall Class"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "Bupa Shortfall Class"].to_dict("records")[0]
  
  with prompt_lib_col4:
    if st.button("BlueCross Usage"):
      prompt = prompt_lib[['type', 'text']].loc[prompt_lib.data == "BlueCross Usage"].to_dict("records")[0]

  prompt_text = st.text_area("Prompt Text", value=prompt, height=200)

  # Move upload controls to sidebar
  with st.sidebar:
    st.header("Upload Loss Ratio PDF")
    uploaded_file = st.file_uploader("Choose pdf...",
                                    type=['pdf']) #type=['png', 'jpg', 'jpeg'])
      
    if uploaded_file is not None:

      #Encode into base64 for data URL input into the AI
      pdf_path = uploaded_file.name
      import tempfile
      import os
      temp_dir = tempfile.mkdtemp()
      pdf_path_full = os.path.join(temp_dir, pdf_path)
      with open(pdf_path_full, "wb") as f:
        f.write(uploaded_file.getvalue())
      base64_pdf = encode_pdf_to_base64(f.name) # this is to get the filepath of the buffered file
      data_url = f"data:application/pdf;base64,{base64_pdf}"
      
      
      binary_data = uploaded_file.getvalue()
      pdf_viewer(input=pdf_path_full, width=700)
      
      if st.button("Extract Text 🔍", type="primary"):
        with st.spinner("Processing image..."):
          try:
            messages = [
              {
                "role": "user",
                "content": [
                            # {"type": "text",
                            # "text":"""
                            # Role: Data Input Officer
                            # Desired Output: a markdown output of the pdf. After that, based on the markdown output, we need to convert it into an .csv format for copy and paste.

                            #   [1st col: policy_id]: Contract Number + "_" + year & month of the starting period.
                            #   [2nd col: policy_number]: Contract Number 
                            #   [3rd col: client_name]: Customer Name
                            #   [4th col: policy_start_date]: the start date of period 
                            #   [5th col: policy_end_date]: one year after the start date of period
                            #   [6th col: duration]: The month between period. If you cannot find that, you can use "Annualised to:". Please make sure that the period may not the same as the policy start date to policy end date. For example, the report is stated as "10/07/2024 to 09/03/2025". The period is 6 months, and the policy start date is 10/07/2024 and policy end date is 09/07/2025.
                            #   [7th col: ibnr]: IBNR, this has to be a percentage. If the report does not state the IBNR, please input "Not State" default.
                            #   [8th col: data_as_of]Data as of
                            #   [9th col: benefit_type] Benefit (Clinical, Dental, Hospital ...., Grand Total etc.)
                            #   [10th col: actual_premium] Actual Subscription
                            #   [11th col: actual_paid_w_ibnr] Actual Claims with IBNR
                            #   [12th col: loss_ratio] Actual Loss Ratio

                            #   The order of this table should be re-arranged by the benefit type. The order is [Hospital, Clinical, Dental, Optical, Maternity, Top-Up/ SMM, Total] (If any one of these benefit type ais not presented in the report, skip the item).
                            #   *Warm reminder when converting*
                            #   1. Some terms are interchangable among different insurers. Below is the reference for you.
                            #     Premium: Subscription
                            #     Customer Name: Client Name/ Policy Holder
                            #     Loss Ratio: Claim Ratio
                            #     Hospital: Hospitalisation/ Inpatient/ Hospital & Surgical
                            #     Clinical: Outpatient
                            #     Actual Premium: Pro-Rata Premium
                            #   2. If the report states "Annualized XXX", ie Annualized Premium, please make sure to convert back to the actual by mulitplying the duration and then divided by 12 months to revert the annualization effect.
                            #   3. The Duration may not be stated by the insurers. Therefore, you shall look into any data period, for example 10/07/2024 to 09/03/2025 where its policy period is 10/07/2024 to 09/07/2025, and then calculate the duration by the number of months between the two dates. In this example, it is 8 months.
                            #   4. Sometimes, there are more than 1 loss ratio reports in the pdf. Please export then all into one markdown table and csv. The order of the table should be first the policy number, then the benefit type.
                            #   5. Date format, if it is in "XX/XX/XXXX" format, it DD/MM/YYYY or D/M/YYYY.
                            #   """},
                              prompt,
                              {"type": "file",
                              "file": {"filename": uploaded_file.name, "file_data": data_url}},

                ]
              }
            ]
            # plugins = [
            #     {
            #         "id": "file-parser",
            #         "pdf": {
            #             "engine": "pdf-text"  # defaults to "mistral-ocr". See Pricing below
            #         }
            #     }

            # ]
            payload = {
              "model": "google/gemini-2.5-flash-preview-05-20",
              #"messages": messages,
              "messages": messages,
              # "plugins": plugins
            }
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code != 200:
              st.error(f"API Error: {response.status_code} - {response.text}")

            # Parse the JSON response
            try:
                result = response.json()
                st.session_state['ocr_result'] = result
                st.success("File processed successfully!")
            except ValueError:
                st.error("Invalid response from API. Could not parse JSON.")
                st.button("response text", response.text)
                st.write("Response content:", response.text)

            st.session_state['ocr_result'] = response.json()
          except Exception as e:
            st.error(f"Error processing image: {str(e)}")
  if 'ocr_result' in st.session_state:
    full_response = response.json()
    st.write(response.json())
    # result_text = full_response.get('choices', [{}])[0].get('message', {}).get('content', '')
    # csv_data = io.StringIO(result_text.split('```csv')[-1].split('```')[0])

    # csv_loss_ratio = pd.read_csv(csv_data, sep=',', header=0, skip_blank_lines=True)
    # st.write(csv_data)
    # st.dataframe(csv_loss_ratio)

    

