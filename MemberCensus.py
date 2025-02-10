import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
#from chart_studio import plotly as py
import plotly.offline as py
import plotly.express as px
import plotly.graph_objects as go


import os

class MemberCensus():
    def __init__(self):
        self.cols = [
            'policy_number',
            'insurer',
            'member_id',
            'dep_type',
            'class',
            'name',
            'gender',
            'age',
            'policy_start_date',
            'staff_id',
            'working_email',
            'client_name',
        ]
        self.col_mapping_setting()
        self.member_df = pd.DataFrame(columns=self.cols)

        self.gender = ['M', 'F']
        self.age_range = np.arange(0, 90, 10)
        # self.age_lbs = ['{} - <{}'.format(i, i+10) for i in self.age_range]
        self.age_lbs = self.age_range[0:-1]
        self.dep_type = ['EE','SP', 'CH']
        
    
    def col_mapping_setting(self):
        self.bupa_cols = [
            'CONT_NO',
            'MBR_NO',
            "MBR_TYPE",
            "CLS_ID	TITLE",
            "NAME",
            "SEX",
            "ACTIVE_CONTRACT_AGE",
            "ACTIVE_CONT_EFF_DATE",
            "RENEWAL_CONTRACT_AGE",
            "RENEWAL_CONT_EFF_DATE",
            "STAFF_NO",
            "DEPT_CODE",
            "OFFICE_EMAIL",
            "REG_DATE",
            "EFF_DATE",
            "PAY_ACCT_NO",
            "STATUS",
            "PAY_METHOD",
            "PAY_TO",
            "CUST_NAME",
        ]
        self.bupa_cols_dtype = {
            'CONT_NO': str,
            'MBR_NO': str,
            "MBR_TYPE": str,
            "CLS_ID": str,
            "TITLE": str,
            "NAME": str,
            "SEX": str,
            "ACTIVE_CONTRACT_AGE": str,
            "ACTIVE_CONT_EFF_DATE": str,
            "RENEWAL_CONTRACT_AGE": str,
            "RENEWAL_CONT_EFF_DATE": str,
            "STAFF_NO": str,
            "DEPT_CODE": str,
            "OFFICE_EMAIL": str,
            "REG_DATE": str,
            "EFF_DATE": str,
            "PAY_ACCT_NO": str,
            "STATUS": str,
            "PAY_METHOD": str,
            "PAY_TO": str,
            "CUST_NAME": str,
        }
        self.bupa_cols_mapping = {
            'CONT_NO': 'policy_number',
            'MBR_NO': 'member_id',
            "MBR_TYPE": 'dep_type',
            "CLS_ID": 'class',
            "NAME": 'name',
            "SEX": 'gender',
            "RENEWAL_CONTRACT_AGE": 'age',
            "RENEWAL_CONT_EFF_DATE": 'policy_start_date',
            "STAFF_NO": 'staff_id',
            "OFFICE_EMAIL": 'working_email',
            "CUST_NAME": 'client_name',
        }


    def get_member_df(self, fp, insurer, password=None):

        if password is not None:
            import msoffcrypto
            import io
            unlocked = io.BytesIO()
            with open(fp, "rb") as file:
                excel_file = msoffcrypto.OfficeFile(file)
                excel_file.load_key(password = password)
                excel_file.decrypt(unlocked)
                from openpyxl import load_workbook
                wb = load_workbook(filename = unlocked)
            temp_df = pd.read_excel(unlocked, dtype=self.bupa_cols_dtype)
        else:
            temp_df = pd.read_excel(fp, dtype=self.bupa_cols_dtype)
        
        if insurer == 'Bupa':
            temp_df.rename(columns=self.bupa_cols_mapping, inplace=True)
            temp_df['insurer'] = insurer
            temp_df = temp_df[self.cols]

        self.member_df = pd.concat([self.member_df, temp_df], axis=0)
        return self.member_df
    
    def member_df_processing(self):
        gender_mapping = {'Male': 'M',
                          'Female': 'F'}
        dep_mapping = {'Employee': 'EE',
                       'Spouse': 'SP',
                       'Child': 'CH',
                       'E': 'EE',
                       'S': 'SP',
                       'C': 'CH',
                       '1': 'EE',
                       '2': 'SP',
                       '3': 'CH',
                       '4': 'CH',
                       '5': 'CH',
                       '6': 'CH',
                       '7': 'CH',
                       '8': 'CH',
                       '9': 'CH',
                       '10': 'CH',}
        self.member_df['gender'].replace(gender_mapping, inplace=True)
        self.member_df['dep_type'].replace(dep_mapping, inplace=True)
        self.member_df['age'] = self.member_df['age'].astype('int')
        return
    
    def get_gender_distribution(self):
        self.gender_dis_df = pd.DataFrame()
        for gender in self.gender:
            for dep in self.dep_type:
                dis = pd.cut(self.member_df['age'].loc[(self.member_df['gender'] == gender) & (self.member_df['dep_type'] == dep)], 
                             bins=self.age_range, 
                             right=False,
                             labels=False
                             )
                self.gender_dis_df = pd.DataFrame(dis.value_counts().sort_index(), columns=[f"{gender}_{dep}"])
                self.gender_dis_df = pd.concat([self.gender_dis_df, temp_df], axis=1, ignore_index=False)
                print(self.gender_dis_df)
        return       

    def butterfly_plot(self, xmax, xstep):
        temp_df = self.gender_dis_df.copy(deep=True)
        temp_df[['M_EE', 'M_SP', 'M_CH']] = -temp_df[['M_EE', 'M_SP', 'M_CH']]

        y = list(range(0, 100, 10))
        # y = self.age_lbs
        tmax = xmax - xstep
        layout = go.Layout(yaxis=go.layout.YAxis(title='Age'),
                            xaxis=go.layout.XAxis(
                            range=[-xmax, xmax],
                            tickvals=np.arange(-tmax, tmax, xstep),
                            ticktext=np.abs(np.arange(-tmax, tmax, xstep)).tolist(),
                            title='Number'),
                            barmode='overlay',
                            bargap=0.1)
        data = [go.Bar(y=y,
                    x=temp_df['M_CH'],
                    orientation='h',
                    hoverinfo='x',
                    name='Men Child',
                    text=-1 * temp_df['M_CH'].astype('int'),
                    marker=dict(color="#7FD9FF")
                    ),
                go.Bar(y=y,
                    x=temp_df['M_SP'],
                    orientation='h',
                    hoverinfo='x',
                    name='Men Spouse',
                    text=-1 * temp_df['M_SP'].astype('int'),
                    opacity=0.5,
                    marker=dict(color="#32C2FF")
                    ),
                go.Bar(y=y,
                    x=temp_df['M_EE'],
                    orientation='h',
                    name='Men Employee',
                    text=-1 * temp_df['M_EE'].astype('int'),
                    hoverinfo='x',
                    opacity=0.5,
                    marker=dict(color="#00B2FF")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_CH'],
                    orientation='h',
                    hoverinfo='x',
                    name='Female Child',
                    marker=dict(color="#FFA69A")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_SP'],
                    orientation='h',
                    hoverinfo='x',
                    name='Female Spouse',
                    opacity=0.5,
                    marker=dict(color="#FF7765")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_EE'],
                    orientation='h',
                    name='Female Employee',
                    hoverinfo='x',
                    opacity=0.5,
                    marker=dict(color="#FF4B33")
                    )
                ]
        fig = go.Figure(data=data, layout=layout)
        return fig