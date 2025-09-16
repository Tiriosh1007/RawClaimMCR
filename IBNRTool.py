import pandas as pd
import numpy as np
import openpyxl 
from openpyxl.styles import Font, Alignment
from dateutil.relativedelta import relativedelta
from openpyxl.styles import PatternFill

class IBNRTool():
    def __init__(self, input_path, number_of_months=8, month_to_extraction=0, col_setup=None):
        if col_setup is None:
            col_setup = ['policy_number', 'insurer', 'policy_start_date', 'policy_end_date', 'incur_date', 'pay_date', 'benefit_type', 'incurred_amount', 'paid_amount']

        self.input_path = input_path
        self.number_of_months = number_of_months
        self.month_to_extraction = month_to_extraction
        self.col_setup = col_setup
        self.benefit_map = {'Hospital': 'IP', 'Clinic': 'OP', 'Dental': 'Dental', 'Maternity': 'Maternity'}

        self.claim_data = None
        

    def read_claim_data(self):
        self.claim_data = pd.read_csv(self.input_path, 
                                      usecols=self.col_setup, 
                                      dtype={'policy_number': str, 'insurer': str, 'policy_start_date': str, 
                                             'policy_end_date': str, 'incur_date': str, 'pay_date': str, 
                                             'benefit_type': str, 'incurred_amount': float, 'paid_amount': float}, 
                                      low_memory=False
                                      )

    def apply_benefit_map(self):
        self.claim_data['benefit_type'] = self.claim_data['benefit_type'].map(self.benefit_map).fillna(self.claim_data['benefit_type'])

    def lag_month(self, row):
        if pd.isna(row['incur_date']) or pd.isna(row['pay_date']):
            return None
        incur = pd.to_datetime(row['incur_date'])
        pay = pd.to_datetime(row['pay_date'])
        diff = relativedelta(pay, incur)
        if diff.years == 0 and diff.months == 0 and diff.days < 0: # definition on lag_month = 0
            return 0
        return diff.years * 12 + diff.months + (0 if diff.days >= 0 else -1)
    
    def prepare_claim_data(self):
        self.claim_data['lag_month'] = self.claim_data.apply(self.lag_month, axis=1)
        self.claim_data = self.claim_data[(self.claim_data['lag_month'].isna()) | (self.claim_data['lag_month'] >= 0)]
        self.claim_data['policy_month'] = pd.to_datetime(self.claim_data['incur_date']).dt.strftime('%Y%m')
        self.claim_data['effective_month'] = pd.to_datetime(self.claim_data['policy_start_date']).dt.month
        self.apply_benefit_map()

        self.claim_data['latest_tri_index'] = pd.to_datetime(self.claim_data['policy_start_date']) + pd.DateOffset(months=self.number_of_months - 1)
        latest_date_after_truncation = self.claim_data.groupby('policy_number')['latest_tri_index'].transform('max')
        self.claim_data['latest_tri_index'] = latest_date_after_truncation
        self.claim_data['latest_tri_index'] = self.claim_data['latest_tri_index'].dt.strftime('%Y%m')
        self.claim_data['latest_tri_index'] = pd.to_numeric(self.claim_data['latest_tri_index'], errors='coerce')
        self.claim_data = self.claim_data.dropna(subset=['latest_tri_index'])

    def run(self):
        self.read_claim_data()
        self.prepare_claim_data()
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            sheet_written = False
            policy_no = self.claim_data['policy_number'].unique()

            # Analyze Each Policy Data
            for pno in policy_no:
                policy_data = self.claim_data[self.claim_data['policy_number'] == pno]
                if policy_data.empty:
                    continue
                for bnf in policy_data['benefit_type'].unique():
                    policy_data = self.claim_data[(self.claim_data['policy_number'] == pno)&(self.claim_data['benefit_type'] == bnf)]
                    latest_tri_month = policy_data['latest_tri_index'].iloc[0]
                    filtered_data = policy_data[policy_data['policy_month'] <= str(latest_tri_month)]
                    if filtered_data.empty:
                        continue
                    loss_triangle, development_factors, total_new_paid, IBNR_factor = self.IBNR_Calculation(filtered_data, pno, bnf)
                    # Write to sheet
                    self._write_sheets_and_format(writer, filtered_data, loss_triangle, development_factors, total_new_paid, IBNR_factor, pno, bnf)
                    sheet_written = True

            # Analyze Overall Data
            for bnf in self.claim_data['benefit_type'].unique():
                bnf_claim_data = self.claim_data[self.claim_data['benefit_type'] == bnf]
                if bnf_claim_data.empty:
                    continue
                overall_latest_tri_month = bnf_claim_data['latest_tri_index'].iloc[0]
                overall_filtered_data = bnf_claim_data[bnf_claim_data['policy_month'] <= str(overall_latest_tri_month)]
                if overall_filtered_data.empty:
                    continue
                loss_triangle, development_factors, total_new_paid, IBNR_factor = self.IBNR_Calculation(overall_filtered_data, 'Overall', bnf)
                self._write_sheets_and_format(writer, overall_filtered_data, loss_triangle, development_factors, total_new_paid, IBNR_factor, 'Overall', bnf)
                sheet_written = True

            if not sheet_written:
                pd.DataFrame({'Info': ['No data available for selected policies']}).to_excel(writer, sheet_name='NoData')

        return output.getvalue()


    def IBNR_Calculation(self, filtered_data, pno, bnf):
        # Generate all months for index with fill
        all_months = pd.date_range(
            start=pd.to_datetime(filtered_data['policy_month'].min(), format='%Y%m'),
            end=pd.to_datetime(filtered_data['policy_month'].max(), format='%Y%m'),
            freq='MS'
        ).to_period('M').astype(str).str.replace('-', '')

        loss_triangle = filtered_data.pivot_table(index='policy_month', columns='lag_month', values='paid_amount', aggfunc='sum', fill_value=0)
        loss_triangle = loss_triangle.reindex(all_months, fill_value=0)
        max_lag_month = int(filtered_data['lag_month'].max()) if filtered_data['lag_month'].notna().any() else 0
        lag_months = list(range(0, max(13, max_lag_month + 1)))
        loss_triangle = loss_triangle.reindex(columns=lag_months, fill_value=0)
        loss_triangle = loss_triangle.cumsum(axis=1)

        max_idx = max(loss_triangle.index.astype(int))

        # Remove projected cells
        for col in loss_triangle.columns.astype(int):
            for idx in loss_triangle.index.astype(int):
                idx_date = pd.to_datetime(str(idx), format='%Y%m')
                max_date = pd.to_datetime(str(max_idx), format='%Y%m')
                if idx_date + pd.DateOffset(months=col) > max_date + pd.DateOffset(months=self.month_to_extraction):
                    loss_triangle.loc[str(idx), col] = np.nan

        development_factors = []
        total_new_paid = []


         # Calculate development factors and avg new paid amount
        for i in range(len(loss_triangle.columns) - 1):
            valid_current = loss_triangle.iloc[:, i].dropna()
            valid_next = loss_triangle.iloc[:, i + 1].dropna()
            valid_indices = valid_current.index.intersection(valid_next.index)
            valid_current = valid_current.loc[valid_indices]
            valid_next = valid_next.loc[valid_indices]

            current_data = valid_current.tail(12)
            next_data = valid_next.tail(12)
            next_data_non_zero = next_data[next_data > 0]

            if current_data.sum() == 0:
                factor = 1
                new_paid = 0
            else:
                factor = next_data.sum() / current_data.sum()
                new_paid = (next_data.sum() - current_data.sum()) / len(next_data_non_zero)

            development_factors.append(factor)
            total_new_paid.append(new_paid)

        # Project future values in loss triangle
        for col in loss_triangle.columns.astype(int):
            for idx in loss_triangle.index.astype(int):
                idx_date = pd.to_datetime(str(idx), format='%Y%m')
                max_date = pd.to_datetime(str(max_idx), format='%Y%m')
                if idx_date + pd.DateOffset(months=col) > max_date + pd.DateOffset(months=self.month_to_extraction):
                    loss_triangle.loc[str(idx), col] = loss_triangle.loc[str(idx), col - 1] * development_factors[col - 1]

        # Calculate IBNR factor
        loss_triangle_no_dev = loss_triangle.copy()
        for col in loss_triangle_no_dev.columns.astype(int):
            for idx in loss_triangle_no_dev.index.astype(int):
                idx_date = pd.to_datetime(str(idx), format='%Y%m')
                max_date = pd.to_datetime(str(max_idx), format='%Y%m')
                if idx_date + pd.DateOffset(months=col) > max_date + pd.DateOffset(months=self.month_to_extraction):
                    loss_triangle_no_dev.loc[str(idx), col] = loss_triangle_no_dev.loc[str(idx), col - 1]

        actual_paid_col = loss_triangle_no_dev.iloc[-self.number_of_months:, -1]
        total_actual_paid = actual_paid_col.sum()
        estimated_paid_col = loss_triangle.iloc[-self.number_of_months:, -1]
        total_estimated_paid = estimated_paid_col.sum()
        IBNR_factor = total_estimated_paid / total_actual_paid - 1

        return loss_triangle, development_factors, total_new_paid, IBNR_factor



    def _write_sheets_and_format(self, writer, filtered_data, loss_triangle, development_factors, total_new_paid, IBNR_factor, pno, bnf):
        sheet_name = f'Triangle_{pno}_{bnf}'
        loss_triangle.to_excel(writer, sheet_name=sheet_name)
        filtered_data.to_excel(writer, sheet_name=f'RawData_{pno}_{bnf}', index=False)

        wb = writer.book
        ws = wb[sheet_name]
        last_row = ws.max_row

        dev_factors_title = ws.cell(row=last_row + 3, column=1, value='Development Factors')
        dev_factors_title.font = Font(bold=True)
        dev_factors_title.alignment = Alignment(horizontal='center')

        for idx, factor in enumerate(development_factors):
            lag_month_title = ws.cell(row=last_row + 2, column=idx + 2, value=f'{idx}/{idx + 1}')
            lag_month_title.font = Font(bold=True)
            lag_month_title.alignment = Alignment(horizontal='center')

            avg_paid_title = ws.cell(row=last_row + 4, column=1, value='Avg New Paid')
            avg_paid_title.font = Font(bold=True)

            dev_factors_display = ws.cell(row=last_row + 3, column=idx + 2, value=factor)
            dev_factors_display.number_format = '#,##0.000'

            avg_paid_display = ws.cell(row=last_row + 4, column=idx + 2, value=total_new_paid[idx])
            avg_paid_display.number_format = '#,##0'

        IBNR_title = ws.cell(row=last_row + 6, column=1, value='IBNR Factor')
        IBNR_title.font = Font(bold=True)

        IBNR_display = ws.cell(row=last_row + 6, column=2, value=IBNR_factor)
        IBNR_display.number_format = '0.00%'

        fill_color = PatternFill(fill_type="solid", fgColor="FFFF00")

        max_idx = max(loss_triangle.index.astype(int))
        for col in loss_triangle.columns.astype(int):
            for idx in loss_triangle.index.astype(int):
                idx_date = pd.to_datetime(str(idx), format='%Y%m')
                max_date = pd.to_datetime(str(max_idx), format='%Y%m')
                if idx_date + pd.DateOffset(months=col) > max_date + pd.DateOffset(months=self.month_to_extraction):
                    cell = ws.cell(row=loss_triangle.index.get_loc(str(idx)) + 2, column=loss_triangle.columns.get_loc(col) + 2)
                    cell.fill = fill_color




    