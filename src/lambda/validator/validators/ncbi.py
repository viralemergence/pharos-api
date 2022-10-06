from .validator import Validator


class NCBI(Validator):
    def validate_type(self):
        if self.dataValue.isnumeric():
            return {"status": self.SUCCESS}

        return {"status": self.FAILURE, "message": ""}

    def validate_format(self):
        if 4 <= len(self.datapoint) <= 8:
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "A valid NCBI ID is 5-8 characters long",
        }

    def validate_referential_integrity(self):
        raise NotImplementedError
