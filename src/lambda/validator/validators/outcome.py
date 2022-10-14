from .validator import Validator


class Detectionoutcome(Validator):
    def validate_type(self):
        if self.data_value.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Valid inputs are Positive/P, Negative/N or Inconclusive/I",
        }

    def validate_format(self):
        return

    def validate_restrictions(self):
        return
