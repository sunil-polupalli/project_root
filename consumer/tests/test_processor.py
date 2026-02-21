import unittest
from app import process_message

class TestProcessor(unittest.TestCase):
    def test_valid_json(self):
        valid_payload = b'{"timestamp": "2023-01-01T00:00:00Z", "message": "test"}'
        result = process_message(valid_payload)
        self.assertTrue(result)

    def test_invalid_json_string(self):
        invalid_payload = b'this is not json'
        result = process_message(invalid_payload)
        self.assertFalse(result)

    def test_invalid_json_format(self):
        invalid_payload = b'{"timestamp": "2023-01-01T00:00:00Z", "message": '
        result = process_message(invalid_payload)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()