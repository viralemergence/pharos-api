from .type import Datapoint


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Validator:
    SUCCESS = "Success"
    FAILURE = "Failure"
    WARNING = "Warning"

    def __init__(self, datapoint: Datapoint) -> None:
        self.__dict__.update(datapoint.get_datapoint())
        self._run_validation()

    def _run_validation(self) -> None:
        if not self.report:
            report = {
                method: getattr(self, method)()
                for method in dir(self)
                if method.startswith("__validate_")
            }
            self.report = report
