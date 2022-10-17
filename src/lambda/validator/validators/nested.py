from .validator import Validator, isfloat


class Nested(Validator):
    def __validate_presence(self):
        if self.dataValue["value"] != "" and self.dataValue["unit"] != "":
            return {"status": self.SUCCESS}

        elif self.dataValue["value"] == "" and self.dataValue["unit"] != "":
            return {
                "status": self.FAILURE,
                "message": "Value missing.",
            }

        elif self.dataValue["value"] != "" and self.dataValue["unit"] == "":
            return {
                "status": self.FAILURE,
                "message": "Unit missing.",
            }

        return {
            "status": self.WARNING,
            "message": "Value and unit missing.",
        }

    def __validate_type(self):
        if isfloat(self.dataValue["value"]):
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non integer values detected",
        }
