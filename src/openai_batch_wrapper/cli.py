import argparse
from openai_batch_wrapper.batch import process_batch

def main():
    parser = argparse.ArgumentParser(description='OpenAI Batch Wrapper CLI')
    parser.add_argument('--input', type=str, required=True, help='Input data for batch processing')
    args = parser.parse_args()
    
    input_data = args.input.split(',')
    results = process_batch(input_data)
    for result in results:
        print(result)

if __name__ == '__main__':
    main() 