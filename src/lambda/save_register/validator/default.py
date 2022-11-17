from .validator import Validator


class Default(Validator):
    def _validate_(self):
        return {"status": self.SUCCESS, "message": "Ready to release."}
