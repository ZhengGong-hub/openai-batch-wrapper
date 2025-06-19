import pandas as pd 
import argparse
import glob
import os

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# relative import 
from openai_batch_wrapper.batch_manager import BatchManager

def main():
    parser = argparse.ArgumentParser(description='Track batch progress for a given job ID')
    parser.add_argument('input_path', type=str, help='The path to the input JSONL file')
    args = parser.parse_args()

    def process_job(job_id, input_jsonl_path):
        batch_manager = BatchManager(job_id=job_id, input_jsonl_path=input_jsonl_path, verbose=False)
        batch_status, status_output = batch_manager.get_batch_status()
        if batch_status == "completed":
            print("Output file: " + str(batch_manager.get_output_file()))
        else:
            print(f"Batch {job_id} is not completed")
        print("--------------------------------\n\n")
        return status_output.tail(1)

    job_paths = glob.glob(os.path.join(args.input_path, 'jsonl/job_*.jsonl'))
    status_outputs = [
        process_job(
            os.path.splitext(os.path.basename(job_path))[0],
            job_path
        )
        for job_path in job_paths
    ]
    print(pd.concat(status_outputs))

if __name__ == "__main__":
    main()