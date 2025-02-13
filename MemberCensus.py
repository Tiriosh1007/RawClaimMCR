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
            # 'staff_id',
            'working_email',
            'client_name',
        ]
        self.col_mapping_setting()
        self.member_df = pd.DataFrame(columns=self.cols)

        self.gender = ['M', 'F']
        # self.age_range = np.arange(0, 100, 10)
        # self.age_lbs = ['{} - <{}'.format(i, i+10) for i in self.age_range]
        # self.age_lbs = self.age_range[0:-1]
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

            #longer version:
            "AGE",
            "CONT_EFF_DATE",
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

            #longer version:
            "AGE": int,
            "CONT_EFF_DATE": str,

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
            # "STAFF_NO": 'staff_id',
            "OFFICE_EMAIL": 'working_email',
            "CUST_NAME": 'client_name',

            #longer version:
            "AGE": 'age',
            "CONT_EFF_DATE": 'policy_start_date',
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

        self.cls = self.member_df['class'].sort_values().unique()
        return
    
    def get_gender_distribution(self):
        self.gender_dis_df, self.gender_dis_dep_df, self.gender_dis_cls_df, self.dis_cls_df, self.dis_cls_dep_df = pd.DataFrame(index=self.age_lbs), pd.DataFrame(index=self.age_lbs), pd.DataFrame(index=self.age_lbs), pd.DataFrame(index=self.age_lbs), pd.DataFrame(index=self.age_lbs)
        self.cls_df = pd.DataFrame(index=self.cls)
        for gender in self.gender:
            dis = pd.cut(self.member_df['age'].loc[(self.member_df['gender'] == gender)], 
                             bins=self.age_range,
                             right=False,
                             labels=self.age_lbs,
                             ).value_counts().sort_index()
            temp_df = pd.DataFrame(dis.values, columns=[f"{gender}"], index=dis.index)
            self.gender_dis_df = pd.concat([self.gender_dis_df, temp_df], axis=1, ignore_index=False)
            for dep in self.dep_type:
                dis_dep = pd.cut(self.member_df['age'].loc[(self.member_df['gender'] == gender) & (self.member_df['dep_type'] == dep)], 
                             bins=self.age_range,
                             right=False, 
                             labels=self.age_lbs,
                             ).value_counts().sort_index()
                temp_df = pd.DataFrame(dis_dep.values, columns=[f"{gender}_{dep}"], index=dis_dep.index)
                # self.gender_dis_df = dis
                self.gender_dis_dep_df = pd.concat([self.gender_dis_dep_df, temp_df], axis=1, ignore_index=False)
                # print(self.gender_dis_df)
            for cls in self.cls:
                dis_cls_gen = pd.cut(self.member_df['age'].loc[(self.member_df['class'] == cls) & (self.member_df['gender'] == gender)], 
                             bins=self.age_range,
                             right=False,
                             labels=self.age_lbs,
                             ).value_counts().sort_index()
                temp_df = pd.DataFrame(dis_cls_gen.values, columns=[f"{gender}_{cls}"], index=dis_cls_gen.index)
                self.gender_dis_cls_df = pd.concat([self.gender_dis_cls_df, temp_df], axis=1, ignore_index=False)
        for cls in self.cls:
            dis_cls = pd.cut(self.member_df['age'].loc[(self.member_df['class'] == cls)], 
                            bins=self.age_range,
                            right=False,
                            labels=self.age_lbs,
                            ).value_counts().sort_index()
            temp_df = pd.DataFrame(dis_cls.values, columns=[f"{cls}"], index=dis_cls.index)
            self.dis_cls_df = pd.concat([self.dis_cls_df, temp_df], axis=1, ignore_index=False)

            for dep in self.dep_type:
                dis_dep = pd.cut(self.member_df['age'].loc[(self.member_df['class'] == cls) & (self.member_df['dep_type'] == dep)], 
                             bins=self.age_range,
                             right=False, 
                             labels=self.age_lbs,
                             ).value_counts().sort_index()
                temp_df = pd.DataFrame(dis_dep.values, columns=[f"{cls}_{dep}"], index=dis_dep.index)
                # self.gender_dis_df = dis
                self.dis_cls_dep_df = pd.concat([self.dis_cls_dep_df, temp_df], axis=1, ignore_index=False)

                # print(self.gender_dis_df)
        for dep in self.dep_type:
            temp_df = self.member_df['class'].loc[self.member_df['dep_type'] == dep].value_counts().rename(f"{dep}").to_frame()
            self.cls_df = pd.concat([self.cls_df, temp_df], axis=1, ignore_index=False)
        
        
        self.gender_dis_df['total'] = self.gender_dis_df.sum(axis=1)
        self.gender_dis_df.loc['total'] = self.gender_dis_df.sum(axis=0)
        self.gender_dis_dep_df['EE_total'] = self.gender_dis_dep_df[['M_EE', 'F_EE']].sum(axis=1)
        self.gender_dis_dep_df['SP_total'] = self.gender_dis_dep_df[['M_SP', 'F_SP']].sum(axis=1)
        self.gender_dis_dep_df['CH_total'] = self.gender_dis_dep_df[['M_CH', 'F_CH']].sum(axis=1)
        self.gender_dis_dep_df.loc['total'] = self.gender_dis_dep_df.sum(axis=0)
        self.cls_df['total'] = self.cls_df.sum(axis=1)
        self.cls_df.loc['total'] = self.cls_df.sum(axis=0)

        return
    
    def get_general_info(self):
        self.ee_age = self.member_df['age'].loc[self.member_df['dep_type'] == 'EE'].mean()
        self.ee_mix_df = self.member_df['gender'].loc[self.member_df['dep_type'] == 'EE'].value_counts()
        self.ee_mix_ratio = self.ee_mix_df['F'] / self.ee_mix_df['M']
        self.total_mix_df = self.member_df['gender'].value_counts()
        self.dep_ratio_df = self.member_df['dep_type'].value_counts()
        self.dep_ratio = 1/ (self.dep_ratio_df.loc['EE'] / self.dep_ratio_df.loc[['SP', 'CH']].sum())
        self.total_member = self.member_df.shape[0]
        return

    def set_graph_layout(self, xmax, xstep, ystep, width, height):
        self.xmax = xmax
        self.xstep = xstep
        self.ystep = ystep
        self.width = width
        self.height = height
        self.age_range = np.arange(0, 100, self.ystep)
        self.age_lbs = self.age_range[0:-1]
        return


    def butterfly_plot(self):
        temp_df = self.gender_dis_df.copy(deep=True)
        temp_df[['M']] = -temp_df[['M']]

        y = list(range(0, 100, self.ystep))
        y_text = [f"{i} - {i+self.ystep-1}" for i in y]
        # y = self.age_lbs
        tmax = self.xmax - self.xstep
        layout = go.Layout(yaxis=go.layout.YAxis(title='Age',
                                                 tickvals=y,
                                                 ticktext=y_text,
                                                 tickfont=dict(size=18),
                                                 ),
                           xaxis=go.layout.XAxis(
                               range=[-self.xmax, self.xmax],
                               tickvals=np.arange(-tmax, tmax, self.xstep),
                               ticktext=np.abs(np.arange(-tmax, tmax, self.xstep)).tolist(),
                               title='Number',
                               showgrid=True,
                               ),
                            title=dict(
                                text='Member Distribution',
                                font = dict(size=20),
                            ),
                            barmode='relative', #barmode='stack',
                            bargap=0.1,
                            width=self.width,
                            height=self.height)
        data = [go.Bar(y=y,
                    x=temp_df['M'],
                    orientation='h',
                    hoverinfo='x',
                    name='Male',
                    text=-1 * temp_df['M'].astype('int'),
                    textfont=dict(size=14),
                    marker=dict(color="#00B4D8")
                    ),
                go.Bar(y=y,
                    x=temp_df['F'],
                    orientation='h',
                    hoverinfo='x',
                    name='Female',
                    text=temp_df['F'].astype('int'),
                    textfont=dict(size=14),
                    marker=dict(color="#F94449")
                    ),
                ]
        fig = go.Figure(data=data, layout=layout)
        return fig

    def butterfly_plot_dep(self):
        temp_df = self.gender_dis_dep_df.copy(deep=True)
        temp_df[['M_EE', 'M_SP', 'M_CH']] = -temp_df[['M_EE', 'M_SP', 'M_CH']]

        y = list(range(0, 100, self.ystep))
        y_text = [f"{i} - {i+self.ystep-1}" for i in y]
        # y = self.age_lbs
        tmax = self.xmax - self.xstep
        layout = go.Layout(yaxis=go.layout.YAxis(title='Age',
                                                 tickvals=y,
                                                 ticktext=y_text,
                                                 tickfont=dict(size=18),
                                                 ),
                           xaxis=go.layout.XAxis(
                               range=[-self.xmax, self.xmax],
                               tickvals=np.arange(-tmax, tmax, self.xstep),
                               ticktext=np.abs(np.arange(-tmax, tmax, self.xstep)).tolist(),
                               title='Number',
                               showgrid=True,
                               ),
                            title=dict(
                                text='Member Distribution by Dependent Type',
                                font = dict(size=20),
                            ),
                            barmode='relative', #barmode='stack',
                            bargap=0.1,
                            width=self.width,
                            height=self.height)
        data = [go.Bar(y=y,
                    x=temp_df['M_EE'],
                    orientation='h',
                    name='Male Employee',
                    text=-1 * temp_df['M_EE'].astype('int'),
                    textfont=dict(size=14),
                    hoverinfo='x',
                    #opacity=0.5,
                    marker=dict(color="#00B4D8")
                    ),
                go.Bar(y=y,
                    x=temp_df['M_SP'],
                    orientation='h',
                    hoverinfo='x',
                    name='Male Spouse',
                    text=-1 * temp_df['M_SP'].astype('int'),
                    textfont=dict(size=14),
                    #opacity=0.5,
                    marker=dict(color="#48CAE4")
                    ),
                go.Bar(y=y,
                    x=temp_df['M_CH'],
                    orientation='h',
                    hoverinfo='x',
                    name='Male Child',
                    text=-1 * temp_df['M_CH'].astype('int'),
                    textfont=dict(size=14),
                    marker=dict(color="#ADE8F4")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_EE'],
                    orientation='h',
                    name='Female Employee',
                    text=temp_df['F_EE'].astype('int'),
                    textfont=dict(size=14),
                    hoverinfo='x',
                    #opacity=0.5,
                    marker=dict(color="#F94449")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_SP'],
                    orientation='h',
                    hoverinfo='x',
                    name='Female Spouse',
                    text=temp_df['F_SP'].astype('int'),
                    textfont=dict(size=14),
                    #opacity=0.5,
                    marker=dict(color="#F69697")
                    ),
                go.Bar(y=y,
                    x=temp_df['F_CH'],
                    orientation='h',
                    hoverinfo='x',
                    name='Female Child',
                    text=temp_df['F_CH'].astype('int'),
                    textfont=dict(size=14),
                    marker=dict(color="#FFCBD1")
                    ),
                ]
        fig = go.Figure(data=data, layout=layout)
        return fig