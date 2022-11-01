from .validator import Validator


class Detectionoutcome(Validator):

    __slot__ = ()

    def _validate_type(self):
        if self.datapoint.dataValue.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Valid inputs are Positive/P, Negative/N or Inconclusive/I",
        }

    def _validate_format(self):
        return

    def _validate_restrictions(self):
        return
