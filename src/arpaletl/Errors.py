class ArpalEtlError(Exception):
    """
    Default exception for arpaletl library
    """

    def __init__(self, message: str):
        """
        Constructor for ResourceError
        """
        self.message = message
        super().__init__(self.message)


class ResourceError(ArpalEtlError):
    """
    Custom exception for resource errors
    """


class ExtractorError(ArpalEtlError):
    """
    Custom exception for extractor errors
    """


class DbClientError(ArpalEtlError):
    """
    Custom exception for db client errors
    """
