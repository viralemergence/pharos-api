from .validator import Validator


class Latin(Validator):
    def validate_type(self):
        value = self.dataValue.replace(" ", "")
        if value.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non-alphabetic characters detected.",
        }

    def validate_format(self):
        if self.dataValue == self.dataValue.capitalize():
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Genus should be capitalized and species should be lowercased.",
        }

    def validate_restrictions(self):
        return

    def validate_referential_integrity(self):
        return
