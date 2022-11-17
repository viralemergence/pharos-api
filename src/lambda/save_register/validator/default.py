from .validator import Validator


class Default(Validator):

    __slot__ = ()

    def _validate_(self):
        return {"status": self.SUCCESS}
