import json
import re
from functools import wraps
import time


class CustomError(Exception):
    pass


def retry(attempts=3, delay=1):
    """
    Retry decorator for functions.

    Args:
        attempts: Number of attempts to retry (default: 3).
        delay: Delay in seconds between attempts (default: 1).
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Attempt {attempt}/{attempts}: Exception occurred: {e}")
                    time.sleep(delay)  # Wait before retrying
            raise  # Re-raise the exception on the last attempt

        return wrapper

    return decorator


def extract_hotel_number(string):
    """Extracts the last integer from a string, handling variations and edge cases.

    Args:
        string: The input string.

    Returns:
        The extracted integer, or None if no valid number is found.
    """

    # Handle common variations:
    if string.endswith("_"):
        string = string[:-1]  # Remove trailing underscore

    # Match a sequence of digits, optionally preceded by a sign:
    match = re.search(r"[-+]?\d+$", string)

    if match:
        # Extract the number and convert to integer:
        return int(match.group())
    else:
        # No valid number found
        return None


def write_extracted_infos(file_path, data):
    """
    Writes the provided data to a JSON file at the specified path.

    Args:
        file_path (str): The path to the output JSON file.
        data (any): The data to be serialized and written to the file.

    Raises:
        Exception: If any errors occur during file opening or JSON serialization.
    """
    try:
        # Open the file in write mode and assign it to json_file
        with open(file_path, "w") as json_file:
            # Serialize the data to JSON format and write it to the file with indentation
            json.dump(data, json_file, indent=4)
    except Exception as e:
        print(f"Error writing data to file: {e}")


def contains_any(text, words):
    """
      Checks if a string 'text' contains any of the words in the list 'words'.

      Args:
          text: The string to search.
          words: A list of words to check for.

      Returns:
          True if the text contains any of the words, False otherwise.
    """
    for word in words:
        if word in text.lower():  # Case-insensitive check
            return True
    return False
