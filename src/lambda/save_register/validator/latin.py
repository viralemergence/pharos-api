from validator import Validator


class Latin(Validator):

    __slot__ = ()

    def _validate_presence(self) -> None:
        if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    def _validate_type(self):
        value = self.datapoint.dataValue.replace(" ", "")
        if value.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non-alphabetic characters detected.",
        }

    def _validate_format(self):
        if self.datapoint.dataValue == self.datapoint.dataValue.capitalize():
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Genus should be capitalized and species should be lowercased.",
        }
