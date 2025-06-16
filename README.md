# OpenAI Batch Wrapper

A simple wrapper for batch processing using OpenAI, managed with UV package manager.

## Setup

1. Ensure you have UV installed.
2. Create a virtual environment and install dependencies:
   ```bash
   uv venv .venv
   source .venv/bin/activate
   uv pip install -e .
   ```

## Usage

Run the CLI with input data:
```bash
python -m openai_batch_wrapper.cli --input "item1,item2,item3"
```

## Development

- Source code is located in `src/openai_batch_wrapper/`.
- Tests are in the `tests/` directory.
- Run tests with:
  ```bash
  python -m unittest discover tests
  ```
