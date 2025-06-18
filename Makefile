.PHONY: help run-track-retrieve

SCRIPT := src/openai_batch_wrapper/track_retrieve_batch_progress.py

help:
	@echo "Available commands:"
	@echo "  make run-track - Run tracker"
	@echo "  make help      - Show help"

track-retrieve:
	uv run $(SCRIPT) $(job_id)