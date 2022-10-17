from .validator import Validator, Datapoint


class NCBI(Validator):
    def __validate_presence(self) -> None:
        if self.dataValue != "":
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    def __validate_type(self):
        if self.dataValue.isnumeric():
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Valid identifiers are integer-only sequences.",
        }

    def __validate_format(self):
        if 0 < len(self.dataValue) < 8:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "A NCBI taxonomic identifier consists of one to seven digits.",
        }
