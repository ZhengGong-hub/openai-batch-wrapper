import pandas as pd

df = pd.read_parquet('input_data/question_transcripts.parquet')

print(df.columns)
print(df[['transcriptpersonid', 'transcriptpersonname']].head())