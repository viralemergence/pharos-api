from .type import Datapoint


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Validator(Datapoint):
    SUCCESS = "Success"
    FAILURE = "Failure"
    WARNING = "Warning"

    def __init__(self, datapoint: dict) -> None:
        super().__init__(datapoint)
        self._run_validation()

    def _run_validation(self) -> None:
        if not hasattr(self, "report"):
            report = {
                method: getattr(self, method)()
                for method in dir(self)
                if method.startswith("_validate_")
            }
            self.report = report
