import pandas as pd 
import time
import glob

# relative import 
from openai_batch_wrapper.batch_manager import BatchManager

job_path = "output_data/small_scale_random_200k/jsonl/job_0.jsonl"

batch_manager = BatchManager(
    job_id="job_0",
    input_jsonl_path=job_path,
    batch_task_reset=False
)

batch_manager.upload_file()
batch_manager.create_batch()
