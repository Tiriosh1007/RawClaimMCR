import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

class ColNameMgnt():
    def __init__(self):
        self.col_df = pd.read_csv('col_mapper.csv')

    def update_col_mapper(self, updated_df):
        temp = pd.concat(self.col_df, updated_df, axis=0, ignore_index=False)
        temp = temp.reset_index().drop(columns=['index'])
        temp.drop_duplicates(subset=['insurer', 'ins_col_name', 'col_name', 'data_type'], inplace=True)

        self.col_df = temp
        self.col_df.to_csv('col_mapper.csv')
        return