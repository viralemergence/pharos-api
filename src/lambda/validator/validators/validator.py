import json


class Validator:
    def __init__(self, datapoint: json):
        self.datapoint = datapoint
        self.dataValue = self.datapoint["dataValue"]
        self.SUCCESS = "Success"
        self.FAILURE = "Failure"
        self.WARNING = "Warning"

    def validate_presence(self):
        """Compels the user to enter data in the required field."""
        if self.dataValue != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Missing value",
        }

    def validate_type(self):
        """Data type."""
        raise NotImplementedError

    def validate_format(self):
        """Entry requieres a specific format."""
        raise NotImplementedError

    def validate_restrictions(self):
        """Field has a definite amount of data that can be entered into them."""
        raise NotImplementedError

    # def validate_referential_integrity(self):
    #     """Impose referential integrity to validate inputs. Check data inputs in certain fields against values in database tables."""
    #     raise NotImplementedError

    def run_validation(self):
        if "report" not in self.datapoint:
            report = {
                method: getattr(self, method)()
                for method in dir(self)
                if method.startswith("validate_")
            }
            self.datapoint["report"] = report
        return self.datapoint
