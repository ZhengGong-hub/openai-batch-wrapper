import os
import time
import json
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
from openai import OpenAI
from pathlib import Path
import duckdb
import pandas as pd
from tqdm import tqdm
from .logger import setup_logger

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import dotenv
dotenv.load_dotenv()

# Set up logger
logger = setup_logger('batch_manager')

class BatchManager:
    """
    A class to manage OpenAI batch processing operations.
    Handles sending requests, waiting for responses, and processing results.
    Uses DuckDB for persistent storage of batch status information.
    """
    
    def __init__(
        self,
        job_id: str,
        input_jsonl_path: str=None,
        output_path: str=None,
        api_key: Optional[str] = None,
        db_path: str = None,
        batch_task_reset: bool = False,
        verbose: bool = True
    ):
        """
        Initialize the BatchManager.
        
        Args:
            job_id: Job ID
            input_jsonl_path: Path to the input JSONL file
            output_path: Path to the output directory
            api_key: OpenAI API key. If None, will look for OPENAI_API_KEY env variable
            db_path: Path to the DuckDB database file
        """
        self.verbose = verbose
        self.job_id = job_id
        if not input_jsonl_path:
            self.input_jsonl_path = None
        else:
            if not os.path.exists(input_jsonl_path):
                raise FileNotFoundError(f"Input JSONL file {input_jsonl_path} not found")
            self.input_jsonl_path = input_jsonl_path
        
        if self.input_jsonl_path:
            with open(self.input_jsonl_path, 'r') as file:
                self.input_jsonl = open(self.input_jsonl_path, "rb")
        
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key must be provided or set as OPENAI_API_KEY environment variable")
        
        self.client = OpenAI(api_key=self.api_key)

        if output_path:
            os.makedirs(output_path, exist_ok=True)
            self.output_path = output_path
        else:
            # infer it from the input_jsonl_path
            self.output_path = os.path.join(os.path.dirname(os.path.dirname(self.input_jsonl_path)), 'output')
            os.makedirs(self.output_path, exist_ok=True)
        
        # Initialize DuckDB connection
        if db_path:
            self.db = duckdb.connect(database=os.path.join(os.path.dirname(db_path), 'batch_status.db'))
        else:
            self.db = duckdb.connect(database=os.path.join(self.output_path, 'batch_status.db'))
        self._init_db(reset=batch_task_reset)

        if self.db.execute("SELECT COUNT(*) FROM batch_status WHERE job_id = ?", (self.job_id,)).fetchone()[0] > 0:
            if self.verbose:
                logger.info(f"Job {self.job_id} already exists in the database")
        else:
            if self.verbose:
                logger.info(f"Job {self.job_id} does not exist in the database")

        try:
            self.batch_input_file_id = [item for item in self.db.execute("SELECT openai_file_id FROM batch_status WHERE job_id = ?", (self.job_id,)).fetchall() if item[0] is not None][0][0]
        except (IndexError, TypeError):
            self.batch_input_file_id = None
            logger.info(f"No input file ID found for job {self.job_id}")

        try:
            self.openai_batch_id = [item for item in self.db.execute("SELECT openai_batch_id FROM batch_status WHERE job_id = ?", (self.job_id,)).fetchall() if item[0] is not None][0][0]
        except (IndexError, TypeError):
            self.openai_batch_id = None
            logger.info(f"No batch ID found for job {self.job_id}")

        try:
            self.openai_output_file_id = [item for item in self.db.execute("SELECT openai_output_file_id FROM batch_status WHERE job_id = ?", (self.job_id,)).fetchall() if item[0] is not None][0][0]
        except (IndexError, TypeError):
            self.openai_output_file_id = None
            logger.info(f"No output file ID found for job {self.job_id}")

        
    def _init_db(self, reset: bool = False):
        if reset:
            self.db.execute("DROP TABLE IF EXISTS batch_status")

        """Initialize the database schema if it doesn't exist."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS batch_status (
                job_id VARCHAR,
                openai_file_id VARCHAR,
                openai_batch_id VARCHAR,
                updated_at TIMESTAMP,
                status VARCHAR,
                message VARCHAR,
                progress CHAR(255),
                openai_output_file_id VARCHAR
            )
        """)
        logger.debug("Initialized database schema")
        
    def _update_batch_status(self, status_data: Dict):
        """
        Update batch status in the database.
        
        Args:
            status_data: Dictionary containing status information with the following keys:
                - job_id: The unique job identifier
                - openai_file_id: The OpenAI file ID (optional)
                - openai_batch_id: The OpenAI batch ID (optional)
                - status: Current status of the job
                - error_message: Any error message (optional)
                - updated_at: Timestamp of the update (optional, will be set if not provided)
        """
        # Ensure we have the required fields
        if 'job_id' not in status_data:
            raise ValueError("job_id is required in status_data")
        if 'status' not in status_data:
            raise ValueError("status is required in status_data")
            
        # Set update timestamp if not provided
        if 'updated_at' not in status_data:
            status_data['updated_at'] = datetime.now()
            
        # Convert the dictionary to a list of values in the correct order
        values = [
            status_data.get('job_id'),
            status_data.get('openai_file_id'),
            status_data.get('openai_batch_id'),
            status_data.get('updated_at', datetime.now()),
            status_data.get('status'),
            status_data.get('message'),
            status_data.get('progress', None),
            status_data.get('openai_output_file_id', None)
        ]
        
        # Insert the data directly
        self.db.execute("""
            INSERT INTO batch_status (
                job_id,
                openai_file_id,
                openai_batch_id,
                updated_at,
                status,
                message,
                progress,   
                openai_output_file_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
        
        logger.debug(f"Updated status for job {status_data['job_id']}: {status_data['status']}")
    
    def upload_file(self) -> str:
        """
        Upload a file to the OpenAI API.

        Returns:
            str: File ID
        """
        if self.batch_input_file_id:
            logger.info(f"File {self.batch_input_file_id} already uploaded")
            return self.batch_input_file_id

        logger.info(f"Uploading file {self.input_jsonl_path} to OpenAI")
        batch_input_file = self.client.files.create(file=self.input_jsonl, purpose='batch')
        self.batch_input_file_id = batch_input_file.id

        # # mock the file id
        # self.batch_input_file_id = "file-VDVo3XAov2WJC4jGyiP9Nd"
        logger.info(f"File uploaded successfully with ID: {self.batch_input_file_id}")
        self._update_batch_status({
            'job_id': self.job_id,
            'openai_file_id': self.batch_input_file_id,
            'openai_batch_id': None,
            'status': 'uploaded',
        })
        return self.batch_input_file_id

    def create_batch(self) -> str:
        """
        Create a new batch processing job.
        
        Returns:
            str: Batch ID
        """
        if self.openai_batch_id:
            logger.info(f"Batch {self.openai_batch_id} already created")
            return self.openai_batch_id
        
        logger.info("Creating new batch processing job")
        batch_object_response = self.client.batches.create(
            input_file_id=self.batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        
        self.openai_batch_id = batch_object_response.id
        current_time = datetime.now()
        
        # Store initial batch status
        self._update_batch_status({
            'job_id': self.job_id,
            'openai_batch_id': self.openai_batch_id,
            'status': batch_object_response.status,
            'message': batch_object_response.errors,
        })
        
        logger.info(f"Created batch {self.openai_batch_id} with status: {batch_object_response.status}")
        return self.openai_batch_id
        

    def get_batch_status(self) -> Dict:
        """
        Get the current status of a batch.
        
        Args:
            batch_id: ID of the batch to check
            
        Returns:
            Dict: Batch status
        """
        logger.info(f"Getting status for batch {self.openai_batch_id} for job {self.job_id}")
        if self.openai_batch_id: # if the batch is created, get the status
            batch_status = self.client.batches.retrieve(self.openai_batch_id)
        else:
            raise ValueError("Cannot get a valid batchid, please check if the batch is created!")

        self.openai_output_file_id = batch_status.output_file_id

        self._update_batch_status({
            'job_id': self.job_id,
            'openai_batch_id': self.openai_batch_id,
            'status': batch_status.status,
            'message': batch_status.errors,
            'progress': 'Completed: ' + str(batch_status.request_counts.completed) + ';' + 'Failed: ' + str(batch_status.request_counts.failed) + ';' + 'Total: ' + str(batch_status.request_counts.total),
            'openai_output_file_id': self.openai_output_file_id
        })
        # return everything in the batch_status.db about this job
        status_output = pd.DataFrame(self.db.execute("SELECT * FROM batch_status WHERE job_id = ?", (self.job_id,)).fetchall(), columns=["job_id", "openai_file_id", "openai_batch_id", "updated_at", "status", "message", "progress", "openai_output_file_id"]).tail(3)
        if self.verbose:
            logger.info(f"Status output: {status_output}")
        return status_output['status'].iloc[-1], status_output # in-progress or completed

    def _regulate_output(self, output_file: str) -> pd.DataFrame:
        """
        Regulate the output file to a pandas dataframe.
        """
        df = pd.read_json(output_file, lines=True)
        flat_df = pd.json_normalize(df.to_dict(orient='records'))
        flat_df.columns = [col.split('.')[-1] for col in flat_df.columns] # get rid of all prefix created due to json_normalize

        flat_df['reponse_message'] = flat_df['choices'].apply(lambda x: x[0]['message']['content'])

        # new columns to create to take the response message
        expanded = flat_df['reponse_message'].apply(json.loads).apply(pd.Series)

        # concat the expanded dataframe to the flat_df
        flat_df = pd.concat([flat_df, expanded], axis=1)

        # keep only the columns relevant to the job
        flat_df = flat_df[['custom_id', 'model', 'prompt_tokens', 'completion_tokens'] + list(expanded.columns)]
        return flat_df

    def get_output_file(self) -> str:
        """
        Get the output file from the OpenAI API.
        
        Returns:
            str: Output file ID
        """
        if self.verbose:
            logger.info(f"Getting output file for job {self.job_id}")
        output_file = self.client.files.content(self.openai_output_file_id)

        # save the output file to a local file
        with open(os.path.join(self.output_path, f"output_{self.job_id}.jsonl"), "w") as f:
            f.write(output_file.text)

        df = self._regulate_output(output_file.text)
        df.to_csv(os.path.join(self.output_path, f"output_{self.job_id}.csv"), index=False)

        return [os.path.join(self.output_path, f"output_{self.job_id}.csv"), os.path.join(self.output_path, f"output_{self.job_id}.jsonl")]

    def cancel_batch(self) -> bool:
        """
        Cancel a running batch.
        
        Args:
            batch_id: ID of the batch to cancel
            
        Returns:
            bool: True if cancellation was successful
        """
        logger.info(f"Cancelling batch {self.openai_batch_id} for job {self.job_id}")
        self.client.batches.cancel(self.openai_batch_id)
        self._update_batch_status({
            'job_id': self.job_id,
            'openai_batch_id': self.openai_batch_id,
            'status': 'cancelled',
            'message': 'Batch cancelled by user'
        })
        logger.info(f"Successfully cancelled batch {self.openai_batch_id} for job {self.job_id}")
        return True

    def delete_all_files(self) -> bool:
        """
        Delete all the files in openai.
        """
        logger.info(f"Deleting all files in openai")

        # get all the files in openai
        files = self.client.files.list()
        logger.info(f"Found {len(files.data)} files in openai")
        for file in tqdm(files.data, desc="Deleting files"):
            self.client.files.delete(file.id)

        return True