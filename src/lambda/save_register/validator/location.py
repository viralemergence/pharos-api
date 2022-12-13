from .validator import Validator, isfloat


class Location(Validator):
    def __init__(self, datapoint, record) -> None:
        self.record = record
        super().__init__(datapoint)

    def _validate_1_presence(self):

        if hasattr(self.record, "Latitude") and hasattr(
            self.record, "Longitude"
        ):  # if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}
        # return {
        #     "status": self.FAILURE,
        #     "message": "Records must have a location.",
        # }

    def _validate_2_type(self):
        if isfloat(self.datapoint.dataValue):
            return {"status": self.SUCCESS}
        return {
            "status": self.FAILURE,
            "message": "Coordinates should be decimal numbers.",
        }

    def _validate_3_format(self):
        sequences = self.datapoint.dataValue.split(".")
        if len(sequences) < 2:
            return {
                "status": self.FAILURE,
                "message": "Coordinates should have 5 decimal numbers separated with a point.",
            }

        if len(sequences[1]) == 5:
            return {"status": self.SUCCESS, "message": "Ready to release."}

        return {
            "status": self.FAILURE,
            "message": "Coordinates should have 5 decimal points.",
        }

    def _validate_4_location(self):
        """Verify if the location is valid"""

        try:
            latitude = float(self.record.Latitude.dataValue)
            longitude = float(self.record.Longitude.dataValue)

            if -90 <= latitude <= 90 and -180 <= longitude <= 180:

                self.record.Latitude.report = {
                    "status": self.SUCCESS,
                    "message": "Ready for release.",
                }
                self.record.Longitude.report = {
                    "status": self.SUCCESS,
                    "message": "Ready for release.",
                }
            else:
                self.record.Latitude.report = {
                    "status": self.FAILURE,
                    "message": "Invalid location.",
                }
                self.record.Longitude.report = {
                    "status": self.FAILURE,
                    "message": "Invalid location.",
                }

        except Exception:
            return {"status": self.FAILURE, "message": "Invalid location."}
