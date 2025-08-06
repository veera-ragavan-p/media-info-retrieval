class SkipValidator:
    """
    A class to determine if full validation should be skipped based on file extension.
    """

    _SKIP_EXTENSIONS = (
        ".stl", ".srt", ".sub", ".sma", ".smi", ".vtt",
        ".atmos", ".metadata", ".wav", ".ec3", ".atmos.audio"
    )


    def should_skip(filepath: str) -> bool:
        """
        Check if the provided file path ends with a known skip extension.

        :param filepath: The S3 filepath to check
        :return: True if validation should be skipped, False otherwise
        """
        return filepath.lower().endswith(SkipValidator._SKIP_EXTENSIONS)


