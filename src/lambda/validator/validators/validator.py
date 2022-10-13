from abc import ABC, abstractmethod
from .types import Datapoint, Record


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Validator(ABC):
    SUCCESS = "Success"
    FAILURE = "Failure"
    WARNING = "Warning"

    def __init__(self, datapoint: Datapoint, record: Record):
        self.datapoint = datapoint
        self.record = record
        self.data_value = self.datapoint.dataValue

    def validate_presence(self):
        if self.data_value != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    @abstractmethod
    def validate_type(self):
        """Data type."""

    @abstractmethod
    def validate_format(self):
        """Entry requieres a specific format."""

    @abstractmethod
    def validate_restrictions(self):
        """Field has a definite amount of data that can be entered into them."""

    @abstractmethod
    def validate_referential_integrity(self):
        """Impose referential integrity to validate inputs. Check data inputs in certain fields against values in database tables."""

    def run_validation(self):
        if "report" not in self.datapoint:
            report = {
                method: getattr(self, method)()
                for method in dir(self)
                if method.startswith("validate_")
            }
            self.datapoint["report"] = report
        return self.datapoint
