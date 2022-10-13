from .validator import Validator, isfloat


class Nested(Validator):
    def validate_presence(self):
        if self.data_value.value != "" and self.data_value.unit != "":
            return {"status": self.SUCCESS}

        elif self.data_value.value == "" and self.data_value.unit != "":
            return {
                "status": self.FAILURE,
                "message": "Value missing.",
            }

        elif self.data_value.value != "" and self.data_value.unit == "":
            return {
                "status": self.FAILURE,
                "message": "Unit missing.",
            }

        return {
            "status": self.WARNING,
            "message": "Value and unit missing.",
        }

    def validate_type(self):
        if isfloat(self.data_value.value):
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non integer values detected",
        }
