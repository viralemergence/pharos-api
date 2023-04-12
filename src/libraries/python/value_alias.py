"""Mappings between user-entered values and database values."""

from enum import Enum


class DetectionOutcome(str, Enum):
    """Allowed values for the detection_outcome
    column in a PublishedRecord.
    """

    POSITIVE = "positive"
    NEGATIVE = "negative"
    INCONCLUSIVE = "inconclusive"


class OrganismSex(str, Enum):
    """Allowed values for the organism_sex
    column in a PublishedRecord.
    """

    MALE = "male"
    FEMALE = "female"
    UNKNOWN = "unknown"


class DeadOrAlive(str, Enum):
    """Allowed values for the dead_or_alive
    column in a PublishedRecord.
    """

    ALIVE = "alive"
    DEAD = "dead"
    UNKNOWN = "unknown"


# Mapping between non-case-sensitive values the user might
# enter in the UI and the database values of a PublishedRecord
DETECTION_OUTCOME_VALUES_MAP: dict[str, DetectionOutcome] = {
    "positive": DetectionOutcome.POSITIVE,
    "negative": DetectionOutcome.NEGATIVE,
    "inconclusive": DetectionOutcome.INCONCLUSIVE,
    "positivo": DetectionOutcome.POSITIVE,
    "negativo": DetectionOutcome.NEGATIVE,
    "inconcluso": DetectionOutcome.INCONCLUSIVE,
    "pos": DetectionOutcome.POSITIVE,
    "neg": DetectionOutcome.NEGATIVE,
    "inc": DetectionOutcome.INCONCLUSIVE,
    "pos.": DetectionOutcome.POSITIVE,
    "neg.": DetectionOutcome.NEGATIVE,
    "inc.": DetectionOutcome.INCONCLUSIVE,
    "1": DetectionOutcome.POSITIVE,
    "0": DetectionOutcome.NEGATIVE,
    "2": DetectionOutcome.INCONCLUSIVE,
    "p": DetectionOutcome.POSITIVE,
    "n": DetectionOutcome.NEGATIVE,
    "i": DetectionOutcome.INCONCLUSIVE,
    "p.": DetectionOutcome.POSITIVE,
    "n.": DetectionOutcome.NEGATIVE,
    "i.": DetectionOutcome.INCONCLUSIVE,
    "+": DetectionOutcome.POSITIVE,
    "-": DetectionOutcome.NEGATIVE,
    "~": DetectionOutcome.INCONCLUSIVE,
}

ORGANISM_SEX_VALUES_MAP: dict[str, OrganismSex] = {
    "male": OrganismSex.MALE,
    "female": OrganismSex.FEMALE,
    "unknown": OrganismSex.UNKNOWN,
    "m": OrganismSex.MALE,
    "f": OrganismSex.FEMALE,
    "u": OrganismSex.UNKNOWN,
    "?": OrganismSex.UNKNOWN,
}

DEAD_OR_ALIVE_VALUES_MAP: dict[str, DeadOrAlive] = {
    "alive": DeadOrAlive.ALIVE,
    "dead": DeadOrAlive.DEAD,
    "unknown": DeadOrAlive.UNKNOWN,
    "a": DeadOrAlive.ALIVE,
    "d": DeadOrAlive.DEAD,
    "u": DeadOrAlive.UNKNOWN,
    "?": DeadOrAlive.UNKNOWN,
}
