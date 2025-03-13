import numpy as np
import pandas as pd
import xlsx

from RawClaimData import *
class StandarRenewal():
    def __init__(self, mcr_fp):
        self.mcr_pages_name = [
            'P.20_Policy',
            'P.20_BenefitType',
            'P.20_Network',
            'P.21_Class',
            'P.22_Class_BenefitType',
            'P.23_IP_Benefit',
            'P.24_OP_Benefit',
            'P.25_Class_Panel_BenefitType',
            'P.26_OP_Panel_Benefit',
        ]
        self.mcr_fp = mcr_fp
        self.worksheet_list = [pd.read_excel(self.mcr_fp, sheet_name=worksheet) for worksheet in self.mcr_pages_name]
        self.font