from .location import Location


class Longitude(Location):
    def validate_format(self, bound=180.0):
        try:
            if abs(float(self.dataValue)) <= bound:
                return {"status": self.SUCCESS}
        except Exception:
            return {
                "status": self.FAILURE,
                "message": f"Longitude values are bounded by -{bound} and +{bound}.",
            }

    # def validate_referential_integrity(self):
    #     return
