
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

import RawClaimData
from st_aggrid import AgGrid, GridUpdateMode, GridOptionsBuilder


st.write("""
# Gain Miles Assurance Consultancy Ltd

### Raw Claim Data Converter and MCR data calculation tool
""")
st.write("---")

upload_file_l = []

uploaded_files = st.file_uploader("Upload raw claim excel .xlsx files", accept_multiple_files=True)
for uploaded_file in uploaded_files:
    # st.write("filename:", uploaded_file.name)
    upload_file_l.append(uploaded_file.name)

file_config = pd.DataFrame(upload_file_l, columns=['File Name'])
file_config['Insurer'] = 'AIA/AXA/Bupa'
file_config['Password'] = None
file_config['Policy start date'] = 'Please input as yyyymmdd format'
file_config['Client Name'] = 'Input Client Name'
file_config['Region'] = 'HK'

st.write('---')
st.header('Raw Claim File Configurations')
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


if st.button('File Configuration Confirm, Proceed'):
  file_config = new_file_config
  st.write('---')
  ben_fp = '/content/benefit_indexing.xlsx'
  raw_ = RawClaimData(ben_fp)
  for n0 in range(len(file_config)):
    raw_.add_raw_data(file_config['File Name'].iloc[n0], 
                      insurer=file_config['Insurer'].iloc[n0], 
                      password=file_config['Password'].iloc[n0], 
                      policy_start_date=file_config['Policy start date'].iloc[n0], 
                      client_name=file_config['Client Name'].iloc[n0], 
                      region=file_config['Region'].iloc[n0])

  raw_.preprocessing()

  st.download_button('MCR data', raw_.mcr_pages(export=True))
  st.download_button('Raw Claim Consolidated', raw_.export_database())





