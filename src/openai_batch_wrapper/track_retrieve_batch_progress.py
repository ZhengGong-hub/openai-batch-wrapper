import pandas as pd 
import argparse

# relative import 
from openai_batch_wrapper.batch_manager import BatchManager

def main():
    parser = argparse.ArgumentParser(description='Track batch progress for a given job ID')
    parser.add_argument('job_id', type=str, help='The job ID to track')
    args = parser.parse_args()

    batch_manager = BatchManager(
        job_id=args.job_id
    )

    batch_status = batch_manager.get_batch_status()
    
    if batch_status == "completed":
        output_file_path = batch_manager.get_output_file()
        print(output_file_path)
    else:
        print(f"Batch {args.job_id} is not completed")

if __name__ == "__main__":
    main()