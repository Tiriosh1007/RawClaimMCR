import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go

import os

class MemberCensus():
    def __init__(self, data):
        self.member_df = self.member_df
        self.cols = [
            'policy_number'
            'insurer',
            'membner_id',
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


    def get_member_df_bupa(self, fp, password=None):

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
            self.member_df = pd.read_excel(unlocked, dtype=self.bupa_cols_dtype)
        else:
            self.member_df = pd.read_excel(fp, dtype=self.bupa_cols_dtype)
        

        return self.member_df