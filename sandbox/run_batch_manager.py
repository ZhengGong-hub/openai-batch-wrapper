import pandas as pd 
import time

# relative import 
from openai_batch_wrapper.batch_manager import BatchManager

batch_manager = BatchManager(
    job_id="job_0_test",
    input_jsonl_path="test_output_data/test1/jsonl/job_0.jsonl",
    batch_task_reset=False
)

batch_manager.upload_file()
batch_manager.create_batch()

# time.sleep(5)
print(batch_manager.get_batch_status())

# batch_manager.cancel_batch()