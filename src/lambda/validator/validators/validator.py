from type import Datapoint


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Validator:

    __slot__ = ("datapoint",)

    SUCCESS = "Success"
    FAILURE = "Failure"
    WARNING = "Warning"

    def __init__(self, datapoint: Datapoint) -> None:
        self.datapoint = datapoint
        self._run_validation()

    def _run_validation(self) -> None:
        if not hasattr(self.datapoint, "report"):
            report = getattr(self, "_presence")()
            if report["status"] == self.SUCCESS:
                report.update(
                    {
                        method: getattr(self, method)()
                        for method in dir(self)
                        if method.startswith("_validate_")
                    }
                )
            setattr(self.datapoint, "report", report)
