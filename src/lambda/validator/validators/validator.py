from abc import ABC, abstractmethod
from .type import Datapoint, Record


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

    def _validate_presence(self) -> None:
        if self.dataValue != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    @abstractmethod
    def _validate_type(self) -> None:
        """Data type."""

    @abstractmethod
    def _validate_format(self) -> None:
        """Entry requieres a specific format."""

    @abstractmethod
    def _validate_restrictions(self) -> None:
        """Field has a definite amount of data that can be entered into them."""

    @abstractmethod
    def _validate_referential_integrity(self) -> None:
        """Impose referential integrity to validate inputs. Check data inputs in certain fields against values in database tables."""

    def run_validation(self) -> None:
        if not self.report:
            report = {
                method: getattr(self, method)()
                for method in dir(self)
                if method.startswith("_validate_")
            }
            self.report = report
