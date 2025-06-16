# in preprocess, we need to: 
# 1. chunk the long dataframe into different jobs. 
# 2. index each row and index the job. 
# 3. save the indexed and chunked jobs somewhere.

import pandas as pd
import uuid
import os
import json

def preprocess_dataframe(df, guiding_prompt, content_col, chunk_size=1000, output_dir='chunks', llm_model='gpt-4o'):
    """
    Preprocess a DataFrame by chunking it, indexing rows and jobs, and saving the results as JSONL files.
    
    Args:
        df (pandas.DataFrame): The input DataFrame to process.
        guiding_prompt (str): The guiding prompt for the OpenAI API.
        content_col (str): The column name of the content to be processed.
        chunk_size (int): The number of rows per chunk.
        output_dir (str): Directory to save the chunked and indexed DataFrames.
    
    Returns:
        list: JSONL file paths
    """

    # check if the output directory exists and if it is not a test directory
    if os.path.exists(output_dir) and 'test' not in output_dir:
        raise FileExistsError(f"Output directory {output_dir} already exists")
    
    # check if the content column exists
    if content_col not in df.columns:
        raise ValueError(f"Content column {content_col} not found in DataFrame")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'indexed_input_data'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'jsonl'), exist_ok=True)

    # reset the index in case the original index is not sequential
    df = df.reset_index(drop=True)
    
    # Add a unique identifier to each row
    df['bash_custom_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Chunk the DataFrame
    df['job_id'] = df.index // chunk_size

    # save the dataframe to a parquet file so we can reference it later
    df.to_parquet(os.path.join(output_dir, 'indexed_input_data', 'indexed_df.parquet'))

    # peek
    print(df)
    
    # create the number of jsonlist based on the number of jobs
    jsonlist = [[] for _ in range(len(df['job_id'].unique()))]

    for _, row in df.iterrows():
        # load in prompt 
        conversations = []
        conversations.append({"role": "system", "content": guiding_prompt})
        conversations.append({"role": "user", "content": row[content_col]})
        jsonlist[row['job_id']].append({ 
            "custom_id": str(row['bash_custom_id']),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": llm_model,
                "temperature": 0,
                "messages": conversations
            }
        })
    
    print('length of jsonlist[0]: ', len(jsonlist[0]), 'and saved to: ', os.path.join(output_dir, 'jsonl', f'job_0.jsonl'))
    print('length of jsonlist[1]: ', len(jsonlist[1]), 'and saved to: ', os.path.join(output_dir, 'jsonl', f'job_1.jsonl'))
    
    # save jsonlists by job_id
    for i, job_id in enumerate(df['job_id'].unique()):
        with open(os.path.join(output_dir, 'jsonl', f'job_{job_id}.jsonl'), 'w') as f:
            for item in jsonlist[i]:
                f.write(json.dumps(item) + '\n')

    return jsonlist