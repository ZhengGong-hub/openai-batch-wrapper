import unittest
from openai_batch_wrapper.batch import process_batch

class TestBatch(unittest.TestCase):
    def test_process_batch(self):
        input_data = ['test1', 'test2']
        results = process_batch(input_data)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], 'Processed: test1')
        self.assertEqual(results[1], 'Processed: test2')

if __name__ == '__main__':
    unittest.main() 