import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings('ignore')

class ColNameMgnt():
    def __init__(self):
        self.col_name = [
            'insurer',
            'ins_col_name',
            'col_name',
            'data_type',
        ]

        self.col_dtype = {
            'insurer': str,
            'ins_col_name': str,
            'col_name': str,
            'data_type': str,
        }
        # self.conn = st.connection("gsheets", type=GSheetsConnection)
        # self.col_df = self.conn.read(
        #     worksheet='Sheet1',
        #     ttl="10m",
        #     usescols=[0, 1, 2, 3],
        #     dtype=self.col_dtype
        # )

        # self.col_df = pd.read_csv('col_mapper.csv', dtype=self.col_dtype)
        self.col_df = pd.read_html("https://docs.google.com/spreadsheets/d/18nqxO0SIYJ2d9r0_Gz1IKySDb-7KdhswpTH8K85dp-o/edit?usp=drive_link", encoding='utf-8')[0]

    def update_col_mapper(self, updated_df):
        temp = pd.concat(self.col_df, updated_df, axis=0, ignore_index=False)
        temp = temp.reset_index().drop(columns=['index'])
        temp.drop_duplicates(subset=['insurer', 'ins_col_name', 'col_name', 'data_type'], inplace=True)
        self.col_df = temp
        self.col_df.to_csv('col_mapper.csv', index=False)
        return

    def import_col_mapper(self, new_mapper):
        self.col_df = new_mapper
        self.col_df.to_csv('col_mapper.csv', index=False)
        return

    def add_col_mapper(self, df_to_add):
        self.col_df = pd.concat([self.col_df, df_to_add], axis=0, ignore_index=False)
        self.col_df.drop_duplicates(subset=['insurer', 'ins_col_name', 'col_name', 'data_type'], inplace=True)
        self.col_df.to_csv('col_mapper.csv', index=False)
        return

    def remove_col_mapper(self, df_to_remove):
        self.col_df = self.col_df[~self.col_df.isin(df_to_remove)].dropna()
        self.col_df.to_csv('col_mapper.csv', index=False)
        return