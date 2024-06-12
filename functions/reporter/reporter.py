import pandas as pd
import boto3
import json

class Reporter():
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.census_df = None
        self.pr_df = None


    def load_data(self, s3_bucket: str):
        self.s3.download_file(s3_bucket, 'census_data.json', './tmp/census_data.json')
        self.s3.download_file(s3_bucket, 'productivity_cost/2024-06-06/pr.data.0.Current', './tmp/pr.data.0.Current')
        self.pr_df = pd.read_csv('./tmp/pr.data.0.Current', sep='\t')

        # Load the census JSON file into a dataframe.
        with open('./tmp/census_data.json') as f:
            data = json.load(f)

        data_to_convert = data['data']
        self.census_df = pd.DataFrame(data_to_convert)
        return self
    
    
    def clean_data(self):
        # Strip whitespace from column names and values
        self.pr_df.columns = self.pr_df.columns.str.strip()
        self.census_df.columns = self.census_df.columns.str.strip()
        self.pr_df = self.pr_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        self.census_df = self.census_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Normalize Dates for 
        self.census_df['Year'] = pd.to_datetime(self.census_df['Year'], format='%Y')
        self.pr_df['year'] = pd.to_datetime(self.pr_df['year'], format='%Y')

        return self


    def print_census_summary(self):
        start_year = pd.to_datetime('2013', format='%Y')
        end_year = pd.to_datetime('2018', format='%Y')

        filtered_census_df = self.census_df[(self.census_df['Year'] >= start_year) & (self.census_df['Year'] <= end_year)]

        population_mean = filtered_census_df['Population'].mean()
        population_std = filtered_census_df['Population'].std()

        print('Mean:', population_mean)
        print('Standard Deviation:', population_std)

        return self
    
    
    def print_pr_summary(self):   
        grouped = self.pr_df.groupby(['series_id', 'year'])['value'].sum()
        grouped = grouped.reset_index()
        best_years = grouped.loc[grouped.groupby('series_id')['value'].idxmax()]

        print("Best years for each series:")
        print(best_years.to_string())


    def print_report(self):
        filtered_pr_df = self.pr_df[(self.pr_df['series_id'] == 'PRS30006032') & (self.pr_df['period'] == 'Q01')]

        merged_df = pd.merge(filtered_pr_df, self.census_df, left_on='year', right_on="Year", how='left')
        report_df = merged_df[['series_id', 'year', 'period', 'value', 'Population']]

        print(report_df.to_string())
    