from .validator import Validator


def isfloat(num: str):
    try:
        float(num)
        return True
    except ValueError:
        return False


class Location(Validator):
    def validate_type(self):
        if isfloat(self.dataValue):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Decimal degrees should be expressed with . and not ,",
        }

    def validate_format(self):
        return

    def validate_restrictions(self, length=8):
        if len(self.dataValue) == length:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Coordinates should have 5 decimal points.",
        }

    # def validate_referential_integrity(self):
    #     raise NotImplementedError
