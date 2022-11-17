from .validator import Validator, isfloat


class Nested(Validator):
    def _validate_presence(self):
        if (
            self.datapoint.dataValue["value"] != ""
            and self.datapoint.dataValue["unit"] != ""
        ):
            return {"status": self.SUCCESS}

        if (
            self.datapoint.dataValue["value"] == ""
            and self.datapoint.dataValue["unit"] != ""
        ):
            return {
                "status": self.FAILURE,
                "message": "Value missing.",
            }

        if (
            self.datapoint.dataValue["value"] != ""
            and self.datapoint.dataValue["unit"] == ""
        ):
            return {
                "status": self.FAILURE,
                "message": "Unit missing.",
            }

        return {
            "status": self.WARNING,
            "message": "Value and unit missing.",
        }

    def _validate_type(self):
        if isfloat(self.datapoint.dataValue["value"]):
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non integer values detected",
        }
