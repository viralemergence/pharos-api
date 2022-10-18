from validator import Validator, Datapoint


class Ncbi(Validator):
    __slot__ = ()

    def _presence(self) -> None:
        if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Missing value",
        }

    def _validate_type(self):
        if self.datapoint.dataValue.isnumeric():
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Valid identifiers are integer-only sequences.",
        }

    def _validate_format(self):
        if 0 < len(self.datapoint.dataValue) < 8:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "A NCBI taxonomic identifier consists of one to seven digits.",
        }
