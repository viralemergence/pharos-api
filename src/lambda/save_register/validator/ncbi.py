from validator import Validator, Datapoint


class Ncbi(Validator):
    __slot__ = ()

    def _validate_1_presence(self):
        if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Missing value",
        }

    def _validate_3_type(self):
        if self.datapoint.dataValue.isnumeric():
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Valid identifiers are integer-only sequences.",
        }

    def _validate_2_format(self):
        if 0 < len(self.datapoint.dataValue) < 8:
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "A NCBI taxonomic identifier consists of one to seven digits.",
        }
