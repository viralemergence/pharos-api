from .location import Location


class Latitude(Location):
    def validate_format(self, bound=90.0):
        try:
            if abs(float(self.dataValue)) <= bound:
                return {"status": self.SUCCESS}
        except Exception:
            return {
                "status": self.FAILURE,
                "message": f"Latitude values are bounded by -{bound} and +{bound}.",
            }

    # def validate_referential_integrity(self):
    #     return
