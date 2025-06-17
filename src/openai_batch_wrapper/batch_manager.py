import os
import time
import json
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
from openai import OpenAI
from pathlib import Path
import duckdb
import pandas as pd
from .logger import setup_logger

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
        input_jsonl_path: str,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: int = 60,
        timeout: int = 86400,  # 24 hours in seconds
        db_path: str = "batch_status.db",
        db_reset: bool = False
    ):
        """
        Initialize the BatchManager.
        
        Args:
            job_id: Job ID
            input_jsonl_path: Path to the input JSONL file
            api_key: OpenAI API key. If None, will look for OPENAI_API_KEY env variable
            max_retries: Maximum number of retries for failed operations
            retry_delay: Delay between retries in seconds
            timeout: Maximum time to wait for batch completion in seconds
            db_path: Path to the DuckDB database file
        """
        self.job_id = job_id
        if not os.path.exists(input_jsonl_path):
            raise FileNotFoundError(f"Input JSONL file {input_jsonl_path} not found")
        
        with open(input_jsonl_path, 'r') as file:
            self.input_jsonl = open(input_jsonl_path, "rb")
        
        self.input_jsonl_path = input_jsonl_path
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key must be provided or set as OPENAI_API_KEY environment variable")
        
        self.client = OpenAI(api_key=self.api_key)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        
        # Initialize DuckDB connection
        self.db = duckdb.connect(db_path)
        self._init_db(reset=db_reset)
        logger.info(f"Initialized BatchManager with database at {db_path}")
        
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
                progress CHAR(255)
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
            status_data.get('progress', None)
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
                progress
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)
        
        logger.debug(f"Updated status for job {status_data['job_id']}: {status_data['status']}")
    
    def upload_file(self) -> str:
        """
        Upload a file to the OpenAI API.

        Returns:
            str: File ID
        """
        logger.info(f"Uploading file {self.input_jsonl_path} to OpenAI")
        # batch_input_file = self.client.files.create(file=self.input_jsonl, purpose='batch')
        # self.batch_input_file_id = batch_input_file.id

        # mock the file id
        self.batch_input_file_id = "file-VDVo3XAov2WJC4jGyiP9Nd"
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
        batch_status = self.client.batches.retrieve(self.openai_batch_id)

        self._update_batch_status({
            'job_id': self.job_id,
            'openai_batch_id': self.openai_batch_id,
            'status': batch_status.status,
            'message': batch_status.errors,
            'progress': 'Completed: ' + str(batch_status.request_counts.completed) + ';' + 'Failed: ' + str(batch_status.request_counts.failed) + ';' + 'Total: ' + str(batch_status.request_counts.total),
        })

        return batch_status
        

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
