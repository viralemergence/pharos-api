from .definitions import Datapoint


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Validator:

    __slot__ = ("datapoint",)

    SUCCESS = "SUCCESS"
    FAILURE = "FAIL"
    WARNING = "WARNING"

    def __init__(self, datapoint: Datapoint) -> None:
        self.datapoint = datapoint
        self._run_validation()

    def _run_validation(self):
        validations = [
            method for method in dir(self) if method.startswith("_validate_")
        ]
        # if not hasattr(self.datapoint, "report"):

        for validation in validations:
            report = getattr(self, validation)()

            if report["status"] == self.FAILURE:
                break

        self.datapoint.report = report
