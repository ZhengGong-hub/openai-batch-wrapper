import pandas as pd
import glob 

output_files = glob.glob('output_data/small_scale_100k/output/output_job_*.csv')

# combine all the output files into one dataframe
df = pd.concat([pd.read_csv(file) for file in output_files])
print(df.head())

# merge on 
indexed = pd.read_parquet('output_data/small_scale_100k/indexed_input_data/indexed_df.parquet')
print(indexed.head())

df = df.merge(indexed, left_on='custom_id', right_on='bash_custom_id', how='left')

# innovation  integrity  quality  respect  teamwork  take mean and std, proid takes count

df_by_employer = df.groupby('companyid').agg({'innovation': ['mean', 'std'], 'integrity': ['mean', 'std'], 'quality': ['mean', 'std'], 'respect': ['mean', 'std'], 'teamwork': ['mean', 'std'], 'proid': 'count', 'companyname': 'first'})
# flatten column name
df_by_employer.columns = ['_'.join(col).strip() for col in df_by_employer.columns.values]

df_by_employer.rename(columns={'companyname_first': 'companyname', 'proid_count': 'count'}, inplace=True)

df_by_employer.set_index('companyname', inplace=True)

# round the result to 3 decimal places
df_by_employer = df_by_employer.round(3)

# keep only the rows where count > 100
df_by_employer = df_by_employer.query('count > 1000')

# sort by count
df_by_employer = df_by_employer.sort_values(by='count', ascending=False)

metrics = ['innovation', 'integrity', 'quality', 'respect', 'teamwork']

# Create mean ± std formatted strings (rounded to 3 decimals)
formatted = {
    m: df_by_employer[f"{m}_mean"].round(2).astype(str) + ' ± ' + df_by_employer[f"{m}_std"].round(2).astype(str)
    for m in metrics
}
formatted = pd.DataFrame(formatted, index=df_by_employer.index)
print(formatted)

# Clean the index by escaping LaTeX special chars
escaped_index = formatted.index.to_series().apply(
    lambda s: s.replace('&', r'\&').replace("Crédit Suisse", r"Cr\'edit Suisse").replace("Research Division", "")
)
formatted.index = escaped_index

latex = formatted.to_latex(
    index=True,
    header=True,
    escape=False,                # prevent pandas from escaping LaTeX
    column_format='l' + 'r' * len(metrics),  # alignment: left for index, right for metrics
)

# output to file
with open('scripts/table.tex', 'w') as f:
    f.write(latex)



# by year 
et_ref = pd.read_csv('input_data/us_et_ref_v2.csv', index_col=0)

# merge on year
df = df.merge(et_ref, left_on='transcriptid', right_on='transcriptid', how='left')
df.dropna(subset=['YearQTR'], inplace=True)
df['year'] = df['YearQTR'].str[:4].astype(int)

# get per year mean and std
df_by_year = df.groupby('year').agg({'innovation': ['mean', 'std'], 'integrity': ['mean', 'std'], 'quality': ['mean', 'std'], 'respect': ['mean', 'std'], 'teamwork': ['mean', 'std'], 'proid': 'count', 'companyname': 'first'})


# flatten column name
df_by_year.columns = ['_'.join(col).strip() for col in df_by_year.columns.values]
df_by_year = df_by_year.query('proid_count > 100')
print(df_by_year)

df_by_year.rename(columns={'companyname_first': 'companyname', 'proid_count': 'count'}, inplace=True)

metrics = ['innovation', 'integrity', 'quality', 'respect', 'teamwork']

# Create mean ± std formatted strings (rounded to 3 decimals)
formatted = {
    m: df_by_year[f"{m}_mean"].round(2).astype(str) + ' ± ' + df_by_year[f"{m}_std"].round(2).astype(str)
    for m in metrics
}
formatted = pd.DataFrame(formatted, index=df_by_year.index)

latex = formatted.to_latex(
    index=True,
    header=True,
    escape=False,                # prevent pandas from escaping LaTeX
    column_format='l' + 'r' * len(metrics),  # alignment: left for index, right for metrics
)

# output to file
with open('scripts/table_by_year.tex', 'w') as f:
    f.write(latex)





