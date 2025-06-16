# in preprocess, we need to: 
# 1. chunk the long dataframe into different jobs. 
# 2. index each row and index the job. 
# 3. save the indexed and chunked jobs somewhere.

import pandas as pd
import uuid
import os
import json
import tiktoken
from .logger import logger

def num_tokens_from_messages(messages, model="gpt-4"):
    """Returns the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        logger.warning("Model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    
    num_tokens = 0
    for message in messages:
        num_tokens += 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":  # if there's a name, the role is omitted
                num_tokens += -1  # role is always required and always 1 token
    num_tokens += 2  # every reply is primed with <im_start>assistant
    return num_tokens

def preprocess_dataframe(df, guiding_prompt, content_col, chunk_size=1000, output_dir='chunks', llm_model='gpt-4'):
    """
    Preprocess a DataFrame by chunking it, indexing rows and jobs, and saving the results as JSONL files.
    
    Args:
        df (pandas.DataFrame): The input DataFrame to process.
        guiding_prompt (str): The guiding prompt for the OpenAI API.
        content_col (str): The column name of the content to be processed.
        chunk_size (int): The number of rows per chunk.
        output_dir (str): Directory to save the chunked and indexed DataFrames.
        llm_model (str): The OpenAI model to use.
    
    Returns:
        list: JSONL file paths
    """
    # check if the output directory exists and if it is not a test directory
    if os.path.exists(output_dir) and 'test' not in output_dir:
        raise FileExistsError(f"Output directory {output_dir} already exists")
    
    # check if the content column exists
    if content_col not in df.columns:
        raise ValueError(f"Content column {content_col} not found in DataFrame")

    logger.info(f"Starting preprocessing with {len(df)} rows")
    
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
    parquet_path = os.path.join(output_dir, 'indexed_input_data', 'indexed_df.parquet')
    df.to_parquet(parquet_path)
    logger.info(f"Saved indexed DataFrame to {parquet_path}")

    # create the number of jsonlist based on the number of jobs
    jsonlist = [[] for _ in range(len(df['job_id'].unique()))]
    total_tokens = 0

    for _, row in df.iterrows():
        # load in prompt 
        conversations = []
        conversations.append({"role": "system", "content": guiding_prompt})
        conversations.append({"role": "user", "content": row[content_col]})
        
        # Calculate tokens for this conversation
        tokens = num_tokens_from_messages(conversations, model=llm_model)
        total_tokens += tokens
        
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
    
    logger.info(f'Total estimated input tokens: {round(total_tokens/1000000, 2)} million tokens')

    # save jsonlists by job_id
    for i, job_id in enumerate(df['job_id'].unique()):
        jsonl_path = os.path.join(output_dir, 'jsonl', f'job_{job_id}.jsonl')
        with open(jsonl_path, 'w') as f:
            for item in jsonlist[i]:
                f.write(json.dumps(item) + '\n')
        logger.info(f'Job {job_id}: {len(jsonlist[i])} items saved to {jsonl_path}')

    logger.info('Preprocessing completed successfully')