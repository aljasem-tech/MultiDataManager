import re
from typing import Optional


class DataCleaner:
    """
    Utility class for cleaning string data.
    """

    @staticmethod
    def cleanup_string(text: Optional[str]) -> Optional[str]:
        """
        Remove all special characters and spaces from the text.

        Args:
            text (Optional[str]): The input text to be cleaned.

        Returns:
            Optional[str]: The cleaned text with only alphanumeric characters, or None if input is None or results in an empty string.
        """
        if text is None:
            return None
        if not isinstance(text, str):
            text = str(text)
        output = re.sub(r'[^a-zA-Z0-9]', '', text)
        return output if output else None

    @staticmethod
    def remove_brackets(text: Optional[str]) -> Optional[str]:
        """
        Remove characters between brackets from the text.

        Args:
            text (Optional[str]): The input text to be processed.

        Returns:
            Optional[str]: The text with bracketed content removed, or None if input is None or results in an empty string.
        """
        if text is None:
            return None
        if not isinstance(text, str):
            text = str(text)
        output = re.sub(r'\(.*?\)', '', text)
        return output.strip() if output else None

    @staticmethod
    def get_between_brackets(text: Optional[str]) -> Optional[str]:
        """
        Get content inside brackets from the text.

        Args:
            text (Optional[str]): The input text to be processed.

        Returns:
            Optional[str]: The content inside brackets, or None if input is None or no brackets found
        """
        if text is None:
            return None
        if not isinstance(text, str):
            text = str(text)
        matches = re.findall(r'\((.*?)\)', text)
        output = ''.join(matches)
        return output.strip() if output else None
