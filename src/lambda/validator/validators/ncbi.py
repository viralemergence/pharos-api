from .validator import Validator, Datapoint


class Ncbi(Validator):
    def _validate_presence(self) -> None:
        if self.dataValue != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    def _validate_type(self):
        if self.dataValue.isnumeric():
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Valid identifiers are integer-only sequences.",
        }

    def _validate_format(self):
        if 0 < len(self.dataValue) < 8:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "A NCBI taxonomic identifier consists of one to seven digits.",
        }
