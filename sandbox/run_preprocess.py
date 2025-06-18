import pandas as pd
from openai_batch_wrapper.preprocess import preprocess_dataframe

# load in the data
df = pd.read_parquet('input_data/question_transcripts.parquet').head(5950)
print(df.head())

# preprocess the data
preprocess_dataframe(
    df=df, 
    guiding_prompt='Categorize the following earnings conference call question: 1. categorize into high integrity, low integrity, or neutral. 2. categorize into high quality, low quality, or neutral.', 
    content_col='componenttext',
    chunk_size=500,
    output_dir='test_output_data/test1/',
    llm_model='gpt-4o',
    structured_output_path='input_data/strucutred_output.json')
