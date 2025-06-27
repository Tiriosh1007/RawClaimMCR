import pandas as pd

class MCR_Convert_prepare:
    def __init__(self):
        col_setup = [
        'policy_id',
        'policy_number',
        'insurer',
        'client_name',
        'policy_start_date',
        'policy_end_date',
        'duration_days',
        'class',
        'benefit_type',
        'benefit',
        'panel',
        'no_of_claims',
        'no_of_claimants',
        'incurred_amount',
        'paid_amount',
        ]
        
        self.col_setup = col_setup
        self.df = pd.DataFrame(columns=self.col_setup)

    def combine_csv_files(csv_101, csv_102, csv_105, output_path):
        if csv_105 is None:
            raise ValueError("csv_105 must be provided")

        df105 = pd.read_csv(csv_105)
        if csv_101:
            df105 = df105.loc[~df105['benefit'].isin([
                "Hospitalization and Surgical Benefits",
                "Top-up/SMM"
            ])]
        if csv_102:
            df105 = df105.loc[~df105['benefit_type'].isin(["OUTPATIENT BENEFITS/CLINICAL"])]

        dfs = []
        if csv_101:
            dfs.append(pd.read_csv(csv_101))
        if csv_102:
            dfs.append(pd.read_csv(csv_102))
        dfs.append(df105)

        combined = pd.concat(dfs, ignore_index=True)

        panel_map = {
            'non-network':       'Non-Panel',
            'network':           'Panel',
            'affiliate-network': 'Panel',
            'affiliate network': 'Panel',
        }
        combined['panel'] = (
            combined['panel']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r'\s+', '-', regex=True)   # e.g. “Affiliate Network” → “affiliate-network”
            .map(panel_map)
            .fillna('non-panel')
        )

        # 5) Extract class digits
        combined['class'] = (
            combined['class']
            .astype(str)
            .str.extract(r'(\d+)', expand=False)
        )

        # 6) Write out and store
        combined.to_csv(output_path, index=False)
        print(f"Combined CSV written to: {output_path}")


def main():
    MCR = MCR_Convert_prepare
    MCR.combine_csv_files(csv_101="101_example.csv", csv_102="102_example.csv", csv_105="105_example.csv", output_path="combined.csv")

if __name__ == '__main__':
    main()
