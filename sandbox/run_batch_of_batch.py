import pandas as pd 
import time
import glob

# relative import 
from openai_batch_wrapper.batch_manager import BatchManager

job_paths = glob.glob("output_data/small_scale_random_200k/jsonl/job_*.jsonl")

for job_path in job_paths:
    batch_manager = BatchManager(
        job_id=job_path.split("/")[-1].split(".")[0],
        input_jsonl_path=job_path,
        batch_task_reset=False
    )

    batch_manager.upload_file()
    batch_manager.create_batch()


# batch_manager.cancel_batch()