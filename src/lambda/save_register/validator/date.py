from .validator import Validator
import datetime


class Date(Validator):
    def __init__(self, datapoint, record) -> None:
        self.record = record
        super().__init__(datapoint)

    def _validate_1_presence(self):
        if (
            hasattr(self.record, "Collection day")
            and hasattr(self.record, "Collection month")
            and hasattr(self.record, "Collection year")
        ):
            return {"status": self.SUCCESS}

        # else:
        #     return {
        #         "status": self.FAILURE,
        #         "message": "Records must have a date.",
        #     }

    def _validate_2_type(self):
        if self.datapoint.dataValue.isdigit():
            return {"status": self.SUCCESS}

        else:
            return {
                "status": self.FAILURE,
                "message": "Dates should be expressed only with digits.",
            }

    def _validate_4_date(self):

        try:
            day = int(getattr(self.record, "Collection day").dataValue)
            month = int(getattr(self.record, "Collection month").dataValue)
            year = int(getattr(self.record, "Collection year").dataValue)

            datetime.date(year, month, day)

            setattr(
                getattr(self.record, "Collection day"),
                "report",
                {"status": self.SUCCESS, "message": "Ready to release."},
            )

            setattr(
                getattr(self.record, "Collection month"),
                "report",
                {"status": self.SUCCESS, "message": "Ready to release."},
            )

            setattr(
                getattr(self.record, "Collection year"),
                "report",
                {"status": self.SUCCESS, "message": "Ready to release."},
            )

        except Exception:
            setattr(
                getattr(self.record, "Collection day"),
                "report",
                {"status": self.FAILURE, "message": "Invalid date."},
            )

            setattr(
                getattr(self.record, "Collection month"),
                "report",
                {"status": self.FAILURE, "message": "Invalid date."},
            )

            setattr(
                getattr(self.record, "Collection year"),
                "report",
                {"status": self.FAILURE, "message": "Invalid date."},
            )
