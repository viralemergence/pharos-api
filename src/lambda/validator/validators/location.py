from .validator import Validator, isfloat


class Location(Validator):
    def _validate_presence(self) -> None:
        if self.dataValue != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Records must have a location.",
        }

    def _validate_type(self):
        if isfloat(self.dataValue):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Decimal degrees should be expressed with . and not ,",
        }

    def _validate_format(self):
        sequences = self.dataValue.split(".")
        if len(sequences[0]) == 5:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Coordinates should have 5 decimal points.",
        }
