import pandas as pd
from openpyxl import load_workbook
import xlsxwriter

class MCR_Convert():
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
            'no_of_claimants': str,
            # 'no_of_claimants': int,
            'incurred_amount': float,
            'paid_amount': float,
        }
        
        self.col_setup = col_setup
        self.df = pd.DataFrame(columns=self.col_setup)

         # files input
        overall_csv = 'combined.csv'
        self.overall_df = pd.read_csv(overall_csv, dtype=self.col_dtype)
        date_col = ['policy_start_date', 'policy_end_date']
        for d in date_col:
            self.overall_df[d] = pd.to_datetime(self.overall_df[d])
        
        self.overall_df['year'] = self.overall_df['policy_start_date'].dt.year

        # 101 only 
        self.inpatient_df = self.overall_df[(self.overall_df['benefit_type'] == "INPATIENT BENEFITS /HOSPITALIZATION") | (self.overall_df['benefit_type'] == "SUPPLEMENTARY MAJOR MEDICAL")]

        # 102 only
        self.outpatient_df = self.overall_df[self.overall_df['benefit_type'] == "OUTPATIENT BENEFITS/CLINICAL"]

    def p20_policy(self):
        self.overall_df['year'] = self.overall_df['policy_start_date'].dt.year

        agg = (self.overall_df.groupby(['policy_number', 'year'], as_index=False).agg(
            incurred_amount=('incurred_amount', 'sum'), paid_amount =('paid_amount', 'sum')))

        agg['usage_ratio'] = agg['paid_amount'] / agg['incurred_amount']
        agg['no_of_claimants'] = ''

        cols = ['policy_number', 'year', 'incurred_amount','paid_amount','no_of_claimants','usage_ratio']
        self.p20_policy_df = agg[cols]
        return self.p20_policy_df

    def p20_benefittype(self):
        final_data = []
        grouped = self.overall_df.groupby(['policy_number', 'year'])

        for (policy_number, year), group_df in grouped:
            hospital_count_df = group_df[group_df['benefit'].str.contains('surgeon', case=False, na=False)]
            hospital_claims_count = len(hospital_count_df)

            hospital_amount_df = self.overall_df[self.overall_df['benefit_type'] == "INPATIENT BENEFITS /HOSPITALIZATION"]
            incurred_amount_hospital = hospital_amount_df['incurred_amount'].sum() 
            paid_amount_hospital = hospital_amount_df['paid_amount'].sum() 
            usage_ratio_hospital = incurred_amount_hospital / paid_amount_hospital if incurred_amount_hospital != 0 else 0
            
            if hospital_claims_count > 0:
                final_data.append({
                    'policy_number': policy_number,
                    'year': year,
                    'benefit_type': 'Hospital',
                    'incurred_amount': incurred_amount_hospital,
                    'paid_amount': paid_amount_hospital,
                    'usage_ratio': usage_ratio_hospital,
                    'no_of_claims': hospital_claims_count,
                    'incurred_per_claim': (incurred_amount_hospital / hospital_claims_count) if hospital_claims_count else '',
                    'paid_per_claim': (paid_amount_hospital / hospital_claims_count) if hospital_claims_count else '',
                    'no_of_claimants': ''})

            clinic_df = group_df[group_df['benefit_type'] == "OUTPATIENT BENEFITS/CLINICAL"]
            dental_df = group_df[group_df['benefit_type'] == "DENTAL"]
            optical_df = group_df[group_df['benefit_type'] == "OPTICAL"]
            
            benefit_types = [
                ('Clinic', clinic_df),
                ('Dental', dental_df),
                ('Optical', optical_df)]
            
            for benefit_name, benefit_df in benefit_types:
                if not benefit_df.empty:
                    incurred_amount = benefit_df['incurred_amount'].sum() if 'incurred_amount' in benefit_df else ''
                    paid_amount = benefit_df['paid_amount'].sum() if 'paid_amount' in benefit_df else ''
                    usage_ratio = incurred_amount / paid_amount if incurred_amount != 0 else 0
                    number_of_claims = len(benefit_df)
                    incurred_per_claim = (incurred_amount / number_of_claims) if number_of_claims else ''
                    paid_per_claim = (paid_amount / number_of_claims) if number_of_claims else ''
                    no_of_claimants = ''  
                    
                    final_data.append({
                        'policy_number': policy_number,
                        'year': year,
                        'benefit_type': benefit_name,
                        'incurred_amount': incurred_amount,
                        'paid_amount': paid_amount,
                        'usage_ratio': usage_ratio,
                        'no_of_claims': number_of_claims,
                        'incurred_per_claim': incurred_per_claim,
                        'paid_per_claim': paid_per_claim,
                        'no_of_claimants': no_of_claimants})

        self.p20_benefittype_df = pd.DataFrame(final_data, columns=[
            'policy_number', 'year', 'benefit_type', 'incurred_amount', 'paid_amount',
            'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim', 'no_of_claimants'])
        return self.p20_benefittype_df

    def p20_network(self):
        agg = (self.overall_df.groupby(['policy_number','year','panel'], as_index=False)
            .agg(incurred_amount=('incurred_amount', 'sum'),paid_amount =('paid_amount', 'sum')))
        agg['usage_ratio'] = agg['paid_amount'] / agg['incurred_amount']

        cols = ['policy_number','year','panel','incurred_amount','paid_amount','usage_ratio']
        self.p20_network_df = agg[cols]

        return self.p20_network_df

    def p20_network_clinic(self):
        self.overall_df['year'] = self.overall_df['policy_start_date'].dt.year

        self.overall_df['bt_low'] = (self.overall_df['benefit_type'].fillna('').str.lower())
        out = self.overall_df[self.overall_df['bt_low']
                    .str.contains('outpatient benefits/clinical', na=False)]

        agg = (out.groupby(['policy_number','year','panel'], as_index=False).agg(
                incurred_amount=('incurred_amount','sum'),
                paid_amount =('paid_amount','sum'),
                no_of_claims =('policy_number','count')))
        
        agg['usage_ratio'] = agg.apply(
            lambda r: r.paid_amount/r.incurred_amount if r.incurred_amount else None,
            axis=1)
        agg = agg.rename(columns={'panel_group':'panel'})
        cols = ['policy_number','year','panel','incurred_amount','paid_amount','no_of_claims','usage_ratio']
        self.p20_network_clinic_df = agg[cols]

        return self.p20_network_clinic_df

    def p21_class(self):
        years = sorted(self.overall_df['year'].dropna().unique())
        if not years:
            return pd.DataFrame(columns=['policy_number','year','class',
                'incurred_amount','paid_amount','no_of_claimants','usage_ratio'])
        elif len(years) == 1:
            current_year = years[0]
            previous_year = None
        else:
            previous_year, current_year = years[0], years[1]

        class_list = self.overall_df['class'].dropna().unique()  
        output_rows = []

        for (policy_num, yr), group in self.overall_df.groupby(['policy_number', 'year']):
            agg = (group.groupby('class').agg({'incurred_amount': 'sum',
                    'paid_amount':'sum'}).to_dict(orient='index'))

            for cls in class_list:
                stats = agg.get(cls, {'incurred_amount': 0, 'paid_amount': 0})
                ti = stats['incurred_amount']
                tp = stats['paid_amount']
                ur = tp / ti if ti else None

                output_rows.append({
                    'policy_number': policy_num,
                    'year': yr,
                    'class': cls,
                    'incurred_amount': ti if ti else None,
                    'paid_amount': tp if tp else None,
                    'no_of_claimants': None,   
                    'usage_ratio': ur,})

        cols = ['policy_number','year','class', 'incurred_amount','paid_amount', 'no_of_claimants','usage_ratio']
        self.p21_class_df = pd.DataFrame(output_rows, columns=cols)
        return self.p21_class_df 

    def p22_class_benefittype(self):
        final_data = []
        grouped = self.overall_df.groupby(['policy_number', 'year', 'class'])

        for (policy_number, year, class_num), group_df in grouped:
            hospital_count_df = group_df[group_df['benefit'].str.contains('surgeon', case=False, na=False)]
            hospital_claims_count = len(hospital_count_df)

            hospital_amount_df = group_df[group_df['benefit_type'] == "INPATIENT BENEFITS /HOSPITALIZATION"]
            incurred_amount_hospital = hospital_amount_df['incurred_amount'].sum() 
            paid_amount_hospital = hospital_amount_df['paid_amount'].sum() 
            usage_ratio_hospital = incurred_amount_hospital / paid_amount_hospital if incurred_amount_hospital != 0 else 0
            
            if hospital_claims_count > 0:
                final_data.append({
                    'policy_number': policy_number,
                    'year': year,
                    'class': class_num,
                    'benefit_type': 'Hospital',
                    'incurred_amount': incurred_amount_hospital,
                    'paid_amount': paid_amount_hospital,
                    'usage_ratio': usage_ratio_hospital,})

            clinic_df = group_df[group_df['benefit_type'] == "OUTPATIENT BENEFITS/CLINICAL"]
            dental_df = group_df[group_df['benefit_type'] == "DENTAL"]
            optical_df = group_df[group_df['benefit_type'] == "OPTICAL"]
            
            benefit_types = [
                ('Clinic', clinic_df),
                ('Dental', dental_df),
                ('Optical', optical_df)]
            
            for benefit_name, benefit_df in benefit_types:
                if not benefit_df.empty:
                    incurred_amount = benefit_df['incurred_amount'].sum() if 'incurred_amount' in benefit_df else ''
                    paid_amount = benefit_df['paid_amount'].sum() if 'paid_amount' in benefit_df else ''
                    usage_ratio = incurred_amount / paid_amount if incurred_amount != 0 else 0
                    
                    final_data.append({
                        'policy_number': policy_number,
                        'year': year,
                        'class': class_num,
                        'benefit_type': benefit_name,
                        'incurred_amount': incurred_amount,
                        'paid_amount': paid_amount,
                        'usage_ratio': usage_ratio,})

        self.p22_class_benefittype_df = pd.DataFrame(final_data, columns=[
            'policy_number', 'year', 'class', 'benefit_type', 'incurred_amount', 'paid_amount','usage_ratio'])

        return self.p22_class_benefittype_df
    
    def p23_ip_benefit(self):
        years = sorted(self.inpatient_df['year'].dropna().unique())
        if not years:
            return pd.DataFrame(columns=['policy_number','year','benefit_type',
                'incurred_amount','paid_amount','usage_ratio',
                'no_of_claims','incurred_per_claims','paid_per_claims','no_of_claimants'])
        elif len(years) == 1:
            current_year = years[0]
            previous_year = None
        else:
            previous_year, current_year = years[0], years[1]

        benefit =  self.inpatient_df['benefit'].unique()
        benefit_lower = [b.lower() for b in benefit]

        output_rows = []
        for (policy_num, yr), df_py in self.inpatient_df.groupby(['policy_number', 'year']):
            
            df_py['bt_norm'] = (df_py['benefit'].fillna('').str.strip().str.lower())

            agg = df_py.groupby('bt_norm').agg({'incurred_amount': 'sum',
                'paid_amount': 'sum', 'no_of_claims': 'sum',}).to_dict(orient='index')

            def get_stats(bt_lower):
                stats = agg.get(bt_lower, {})
                ti = stats.get('incurred_amount', 0)
                tp = stats.get('paid_amount', 0)
                nc = stats.get('no_of_claims', 0)
                return ti, tp, nc

            for orig_bt, bt_lower in zip(benefit, benefit_lower):
                ti, tp, nc = get_stats(bt_lower)
                ur = tp/ti if ti else None #usage ratio
                ip = ti/nc if nc else None # incurred per case
                pp = tp/nc if nc else None # paid per case 

                output_rows.append({'policy_number': policy_num,
                    'year': yr,
                    'benefit': orig_bt,
                    'incurred_amount': ti if ti else None,
                    'paid_amount': tp if tp else None,
                    'usage_ratio': ur,
                    'no_of_claims': nc if nc else None,
                    'no_of_claimants': None,
                    'incurred_per_case': ip,
                    'paid_per_case': pp,
                    'incurred_per_claimant': None, 
                    'paid_per_claimant': None,
                    'claim_frequency':  None })

        cols = ['policy_number','year','benefit','incurred_amount','paid_amount',
            'usage_ratio','no_of_claims','no_of_claimants','incurred_per_case','paid_per_case','incurred_per_claimant','paid_per_claimant','claim_frequency']
        self.p23_IP_benefit_df = pd.DataFrame(output_rows, columns=cols)

        return self.p23_IP_benefit_df

    def p23a_class_IP_benefit(self):
        final_data = []
        grouped = self.inpatient_df.groupby(['policy_number', 'year', 'class'])
        benefit_list = self.inpatient_df['benefit'].dropna().unique()

        for (policy_number, year, class_num), group_df in grouped:
            agg = (group_df.groupby('benefit').agg({'incurred_amount': 'sum', 'paid_amount': 'sum'}).reset_index())

            agg_dict = agg.set_index('benefit').to_dict(orient='index')

            for benefit in benefit_list:
                if benefit in agg_dict:
                    data = agg_dict[benefit]
                    ti = data['incurred_amount']
                    tp = data['paid_amount']
                    count = len(group_df[group_df['benefit'] == benefit])

                    ur = tp / ti if ti != 0 else None
                    incurred_per_claim = (ti / count) if count else None
                    paid_per_claim = (tp / count) if count else None

                    final_data.append({
                        'policy_number': policy_number,
                        'year': year,
                        'class': class_num,
                        'benefit': benefit,
                        'incurred_amount': ti,
                        'paid_amount': tp,
                        'usage_ratio': ur,
                        'no_of_claims': count,
                        'no_of_claimants': None,  
                        'incurred_per_case': incurred_per_claim,
                        'paid_per_case': paid_per_claim,
                        'incurred_per_claimant': None,
                        'paid_per_claimant': None,
                        'claim_frequency': None})

        self.p23a_class_IP_benefit_df = pd.DataFrame(final_data, columns=[
            'policy_number','year','class','benefit','incurred_amount','paid_amount',
            'usage_ratio','no_of_claims','no_of_claimants','incurred_per_case',
            'paid_per_case','incurred_per_claimant','paid_per_claimant','claim_frequency'])

        return self.p23a_class_IP_benefit_df
    
    def p24_op_benefit(self):
        years = sorted(self.outpatient_df['year'].dropna().unique())
        if not years:
            return pd.DataFrame(columns=['policy_number','year','benefit',
            'incurred_amount','paid_amount','no_of_claimants','usage_ratio'])
        elif len(years) == 1:
            current_year = years[0]
            previous_year = None
        else:
            previous_year, current_year = years[0], years[1]

        output_rows = []
        benefit_list = self.outpatient_df['benefit'].dropna().unique()

        for (policy_num, yr), group in self.outpatient_df.groupby(['policy_number', 'year']):
            agg = (group.groupby('benefit').agg({'incurred_amount': 'sum',
                    'paid_amount': 'sum'}).to_dict(orient='index'))

            for benefit in benefit_list:
                stats = agg.get(benefit, {'incurred_amount': 0, 'paid_amount': 0})
                ti = stats['incurred_amount']
                tp = stats['paid_amount']
                ur = tp / ti if ti else None

                subset = group[group['benefit'] == benefit]
                count = len(subset)

                incurred_per_claim = (ti / count) if count else None
                paid_per_claim = (tp / count) if count else None

                output_rows.append({
                    'policy_number': policy_num,
                    'year': yr,
                    'benefit': benefit,
                    'incurred_amount': ti if ti else None,
                    'paid_amount': tp if tp else None,
                    'usage_ratio': ur,
                    'no_of_claims': count,
                    'incurred_per_claim': incurred_per_claim,
                    'paid_per_claim': paid_per_claim,
                    'no_of_claimants': None})

        self.p24_op_benefit_df = pd.DataFrame(output_rows, columns=[
            'policy_number', 'year', 'benefit', 'incurred_amount', 'paid_amount',
            'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim', 'no_of_claimants'])

        return self.p24_op_benefit_df
        
    def p25_class_panel_benefittype(self):
        final_data = []
        grouped = self.overall_df.groupby(['policy_number', 'year', 'class', 'panel'])

        for (policy_number, year, class_num, panel), group_df in grouped:
            hospital_count_df = group_df[group_df['benefit'].str.contains('surgeon', case=False, na=False)]
            hospital_claims_count = len(hospital_count_df)

            hospital_amount_df = group_df[group_df['benefit_type'] == "INPATIENT BENEFITS /HOSPITALIZATION"]
            incurred_amount_hospital = hospital_amount_df['incurred_amount'].sum() 
            paid_amount_hospital = hospital_amount_df['paid_amount'].sum() 
            usage_ratio_hospital = incurred_amount_hospital / paid_amount_hospital if incurred_amount_hospital != 0 else 0
            
            if hospital_claims_count > 0:
                final_data.append({
                    'policy_number': policy_number,
                    'year': year,
                    'class': class_num,
                    'panel': panel,
                    'benefit_type': 'Hospital',
                    'incurred_amount': incurred_amount_hospital,
                    'paid_amount': paid_amount_hospital,
                    'usage_ratio': usage_ratio_hospital,})

            clinic_df = group_df[group_df['benefit_type'] == "OUTPATIENT BENEFITS/CLINICAL"]
            dental_df = group_df[group_df['benefit_type'] == "DENTAL"]
            optical_df = group_df[group_df['benefit_type'] == "OPTICAL"]
            
            benefit_types = [
                ('Clinic', clinic_df),
                ('Dental', dental_df),
                ('Optical', optical_df)]
            
            for benefit_name, benefit_df in benefit_types:
                if not benefit_df.empty:
                    incurred_amount = benefit_df['incurred_amount'].sum() if 'incurred_amount' in benefit_df else ''
                    paid_amount = benefit_df['paid_amount'].sum() if 'paid_amount' in benefit_df else ''
                    usage_ratio = incurred_amount / paid_amount if incurred_amount != 0 else 0
                    
                    final_data.append({
                        'policy_number': policy_number,
                        'year': year,
                        'class': class_num,
                        'panel': panel,
                        'benefit_type': benefit_name,
                        'incurred_amount': incurred_amount,
                        'paid_amount': paid_amount,
                        'usage_ratio': usage_ratio,})

        self.p25_class_panel_benefittype_df = pd.DataFrame(final_data, columns=[
            'policy_number', 'year', 'class', 'panel','benefit_type', 'incurred_amount', 'paid_amount','usage_ratio'])
        
        return self.p25_class_panel_benefittype_df
    
    def p26_op_panel_benefit(self):
        years = sorted(self.outpatient_df['year'].dropna().unique())
        if not years:
            return pd.DataFrame(columns=['policy_number','year','benefit',
            'incurred_amount','paid_amount','no_of_claimants','usage_ratio'])
        elif len(years) == 1:
            current_year = years[0]
            previous_year = None
        else:
            previous_year, current_year = years[0], years[1]

        output_rows = []
        benefit_list = self.outpatient_df['benefit'].dropna().unique()

        for (policy_num, yr, panel), group in self.outpatient_df.groupby(['policy_number', 'year', 'panel']):
            agg = (group.groupby('benefit').agg({'incurred_amount': 'sum','paid_amount': 'sum'}).reset_index())
        
            agg_dict = agg.set_index('benefit').to_dict(orient='index')

            for benefit in benefit_list:
                if benefit in agg_dict:
                    data = agg_dict[benefit]
                    ti = data['incurred_amount']
                    tp = data['paid_amount']
                    count = len(group[group['benefit'] == benefit])
                    ur = tp / ti if ti else None
                    incurred_per_claim = (ti / count) if count else None
                    paid_per_claim = (tp / count) if count else None

                    output_rows.append({
                        'policy_number': policy_num,
                        'year': yr,
                        'panel': panel,
                        'benefit': benefit,
                        'incurred_amount': ti if ti else None,
                        'paid_amount': tp if tp else None,
                        'usage_ratio': ur,
                        'no_of_claims': count,
                        'incurred_per_claim': incurred_per_claim,
                        'paid_per_claim': paid_per_claim,
                        'no_of_claimants': None})

        self.p26_op_panel_benefit_df = pd.DataFrame(output_rows, columns=[
            'policy_number', 'year', 'panel', 'benefit', 'incurred_amount', 'paid_amount',
            'usage_ratio', 'no_of_claims', 'incurred_per_claim', 'paid_per_claim', 'no_of_claimants'])

        return self.p26_op_panel_benefit_df
    
    def write_excel(self):
        with pd.ExcelWriter("test.xlsx", engine='xlsxwriter') as writer:
            self.p20_policy_df.to_excel(writer, sheet_name='P.20_Policy', index=False)
            self.p20_benefittype_df.to_excel(writer, sheet_name='P.20_BenefitType', index =False)
            self.p20_network_df.to_excel(writer, sheet_name='P.20_Network', index=False)
            self.p20_network_clinic_df.to_excel(writer, sheet_name='P.20_Network_Clinic', index=False)
            self.p21_class_df.to_excel(writer, sheet_name="P.21_Class", index=False)
            self.p22_class_benefittype_df.to_excel(writer, sheet_name="P.22_Class_BenefitType", index=False)
            self.p23_IP_benefit_df.to_excel(writer, sheet_name="P.23_IP_Benefit", index=False)
            self.p23a_class_IP_benefit_df.to_excel(writer, sheet_name='P.23a_Class_IP_Benefit', index=False)
            self.p24_op_benefit_df.to_excel(writer, sheet_name='P.24_OP_Benefit', index=False)
            self.p25_class_panel_benefittype_df.to_excel(writer, sheet_name="P.25_Class_Panel_BenefitType", index=False)
            self.p26_op_panel_benefit_df.to_excel(writer, sheet_name='P.26_OP_Panel_Benefit',index=False)

def main():
    MCR = MCR_Convert()
    MCR.p20_policy()
    MCR.p20_benefittype()
    MCR.p20_network()
    MCR.p20_network_clinic()
    MCR.p21_class()
    MCR.p22_class_benefittype()
    MCR.p23_ip_benefit()
    MCR.p23a_class_IP_benefit()
    MCR.p24_op_benefit()
    MCR.p25_class_panel_benefittype()
    MCR.p26_op_panel_benefit()
    MCR.write_excel()

if __name__ == '__main__':
    main()