from .validator import Validator
import datetime


class Date(Validator):
    def __init__(self, datapoint, record) -> None:
        self.record = record
        super().__init__(datapoint)

    def _validate_1_presence(self):
        if hasattr(self.datapoint, "dataValue"):
            return {"status": self.SUCCESS}

        else:
            return {
                "status": self.FAILURE,
                "message": "Records must have a date.",
            }

    def _validate_2_type(self):
        if self.datapoint.dataValue.isdigit():
            return {"status": self.SUCCESS}

        else:
            return {
                "status": self.FAILURE,
                "message": "Dates should be expressed only with digits.",
            }

    def _validate_4_date(self):
        if (
            hasattr(self.record, "Collection day")
            and hasattr(self.record, "Collection month")
            and hasattr(self.record, "Collection year")
        ):

            try:
                day = int(getattr(self.record, "Collection day").dataValue)
                month = int(getattr(self.record, "Collection month").dataValue)
                year = int(getattr(self.record, "Collection year").dataValue)

                date_ = datetime.date(
                    year=year,
                    month=month,
                    day=day,
                )

                return {"status": self.SUCCESS, "message": "Ready to release."}

            except Exception:
                return {"status": self.FAILURE, "message": "Invalid date."}

        return {"status": self.FAILURE, "message": "Invalid date, missing values."}
