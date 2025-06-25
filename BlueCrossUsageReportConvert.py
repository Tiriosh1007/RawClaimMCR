import pandas as pd

class BlueCrossUsageReportConvert:
    def __init__(self, input_file):
        self.col = [
            "policy_id",
            "policy_number",
            "insurer",
            "client_name",
            "policy_start_date",
            "policy_end_date",
            "duration_days",
            "class",
            "benefit_type",
            "benefit",
            "panel",
            "no_of_claims",
            "no_of_claimants",
            "incurred_amount",
            "paid_amount"
        ]

        self.col_dtype = {
            'policy_id': str,
            'policy_number': str,
            'insurer': str,
            'client_name': str,
            'policy_start_date': str,
            'policy_end_date': str,
            'duration_days': int,
            'class': str,
            'benefit_type': str,
            'benefit': str,
            'panel': str,
            'no_of_claims': int,
            'no_of_claimants': int,
            'incurred_amount': float,
            'paid_amount': float,
        }

        input_col_dtype = {
            "Policy number": str,
            "Start date": str,
            "End date": str,
            "Class": str, 
            "Benefit Description": str, 
            "Number of Claim Lines": int, 
            "Presented": float,  
            "Adjusted": float, 
        }

        self.df = pd.read_csv(input_file, sep=',') 
        self.df.fillna(0, inplace=True)
        self.df.replace('-', 0, inplace=True)
        columns_to_convert = ['Presented', 'Adjusted']
        for col in columns_to_convert:
            self.df[col] = self.df[col].astype(str).str.replace(',', '', regex=False)
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        self.df.astype(input_col_dtype)
        self.df['Start date'] = pd.to_datetime(self.df['Start date'])
        self.df['End date'] = pd.to_datetime(self.df['End date'])
        self.df = self.df[~self.df['Benefit Description'].str.contains('Total')]

    def csv_convert(self):
        # print(self.df)
        final_df = []

        for idx, row in self.df.iterrows():
            contract_num = row['Policy number']
            start_date = row['Start date']
            end_date = row['End date']
            start_year = pd.to_datetime(start_date).year
            policy_holder = row['Client name']
            benefit, class_num = row['Class'].split()

            data_row = {
                'policy_id': f"{contract_num}_{start_year}",
                'policy_number': contract_num,
                'insurer': "Blue Cross",
                'client_name': policy_holder,
                'policy_start_date': row['Start date'],
                'policy_end_date': row['End date'],
                'duration_days':  pd.to_datetime(end_date) - pd.to_datetime(start_date) + pd.Timedelta(days=1),
                'class': class_num,
                'benefit_type': benefit,
                'benefit': row['Benefit Description'],
                'panel': None,
                'no_of_claims': row["Number of Claim Lines"],
                'no_of_claimants': None,
                'incurred_amount': row['Presented'],
                'paid_amount': row['Adjusted'],
            }
            final_df.append(data_row)
        final_df = pd.DataFrame(final_df)
        self.final_df = final_df
        # output_file = 'output.csv'
        # final_df.to_csv(output_file, index=False)
        # print("done")

# def main():
#     BlueCross = BlueCrossUsageReportConvert(input_file="claims_summary (1).csv")
#     BlueCross.csv_convert()

# if __name__ == main():
#     main()