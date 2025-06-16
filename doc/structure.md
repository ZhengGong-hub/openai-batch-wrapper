the openai-batch has several steps 

1. preprocess the data in order to produce a list of jsonl file, need: uuid
2. send over to openai
3. wait for 24hours for reply
4. catch the reply 
5. postprocess the results

Wait for 24 hours is problematic, we need to save the relevant ref-files and ids properly.

