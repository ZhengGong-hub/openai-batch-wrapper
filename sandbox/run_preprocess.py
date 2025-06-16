import pandas as pd
from openai_batch_wrapper.preprocess import preprocess_dataframe

# load in the data
df = pd.read_parquet('input_data/question_transcripts.parquet').head(59500)
print(df.head())

# preprocess the data
preprocess_dataframe(
    df=df, 
    guiding_prompt='Categorize the following earnings conference call question into friendly, neutral, or hostile.', 
    content_col='componenttext',
    chunk_size=30000,
    output_dir='test_output_data/test1/',
    llm_model='gpt-4o')
