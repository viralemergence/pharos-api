from .validator import Validator


class Detectionoutcome(Validator):
    def __validate_type(self):
        if self.dataValue.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Valid inputs are Positive/P, Negative/N or Inconclusive/I",
        }

    def __validate_format(self):
        return

    def __validate_restrictions(self):
        return
