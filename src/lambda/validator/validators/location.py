from .validator import Validator, isfloat


class Location(Validator):
    def validate_type(self):
        if isfloat(self.dataValue):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Decimal degrees should be expressed with . and not ,",
        }

    def validate_format(self):
        sequences = self.dataValue.split(".")
        if len(sequences[0]) == 5:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Coordinates should have 5 decimal points.",
        }

    def validate_restrictions(self):
        if self.record.longitude != "" and self.record.latitude != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Longitude or latitude missing.",
        }
