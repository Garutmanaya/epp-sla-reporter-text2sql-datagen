
from enum import Enum

class AugmentationStyle(Enum):
    SHORT = "short"
    NATURAL = "natural"
    VERBOSE = "verbose"
    ALL = "all"

    @classmethod
    def from_str(cls, value):
        mapping = {
            "1": cls.SHORT, "short": cls.SHORT,
            "2": cls.NATURAL, "natural": cls.NATURAL,
            "3": cls.VERBOSE, "verbose": cls.VERBOSE,
            "4": cls.ALL, "all": cls.ALL
        }
        return mapping.get(str(value).lower(), cls.NATURAL)
