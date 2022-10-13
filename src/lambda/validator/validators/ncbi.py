from .validator import Validator


class NCBI(Validator):
    def validate_type(self):
        if self.dataValue.isnumeric():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Valid identifiers are integer-only sequences.",
        }

    def validate_format(self):
        if 0 < len(self.datapoint) < 8:
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "A NCBI taxonomic identifier consists of one to seven digits.",
        }
