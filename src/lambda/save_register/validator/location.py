from .validator import Validator, isfloat


class Location(Validator):
    def __init__(self, datapoint, record) -> None:
        self.record = record
        super().__init__(datapoint)

    def _validate_1_presence(self):

        if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Records must have a location.",
        }

    def _validate_2_type(self):
        if isfloat(self.datapoint.dataValue):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Decimal degrees should be expressed with . and not ,",
        }

    def _validate_3_format(self):
        sequences = self.datapoint.dataValue.split(".")
        if len(sequences) < 2:
            return {
                "status": self.FAILURE,
                "message": "Coordinates should have decimal points.",
            }

        if len(sequences[1]) == 5:
            return {"status": self.SUCCESS, "message": "Ready to release."}
        return {
            "status": self.FAILURE,
            "message": "Coordinates should have 5 decimal points.",
        }

    def _validate_4_location(self):
        """Verify if the location is valid"""

        if hasattr(self.record, "Latitude") and hasattr(self.record, "Longitude"):

            try:

                latitude = float(self.record.Latitude.dataValue)
                longitude = float(self.record.Longitude.dataValue)

                if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    return {"status": self.SUCCESS, "message": "Ready for release."}

            except Exception:
                return {"status": self.FAILURE, "message": "Invalid location."}

        return {"status": self.FAILURE, "message": "Invalid location, missing values."}
