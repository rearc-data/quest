from functions.reporter import reporter as r
import pandas as pd

def test_reporter():
    reporter = r.Reporter()

    reporter.pr_df = pd.DataFrame({
        'series_id': ['   PRS30006032    '] * 5,
        'year     ': ['2013', '2014', '2015   ', '   2016', '   2017   '],
        'period': ['Q01   '] * 5,
        '   value   ': [1.9, 2.0, 2.1, 2.2, 2.3],
    })
    reporter.census_df = pd.DataFrame({
        'Year': ['2013   ', '   2014', '   2015   ', '2016', '2017'],
        '     Population': [1000, 2000, 3000, 4000, 5000],
    })

    reporter.clean_data()
    assert reporter.pr_df['year'].dtype == 'datetime64[ns]'
    assert reporter.pr_df.columns.tolist() == ['series_id', 'year', 'period', 'value']
    assert reporter.pr_df['series_id'].tolist() == ['PRS30006032'] * 5
    assert reporter.pr_df['period'].tolist() == ['Q01'] * 5
    assert reporter.pr_df['value'].tolist() == [1.9, 2.0, 2.1, 2.2, 2.3]

    assert reporter.census_df['Year'].dtype == 'datetime64[ns]'
    assert reporter.census_df.columns.tolist() == ['Year', 'Population']
    assert reporter.census_df['Population'].tolist() == [1000, 2000, 3000, 4000, 5000]
    assert reporter.census_df['Population'].dtype == 'int64'



