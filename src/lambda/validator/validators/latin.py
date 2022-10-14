from .validator import Validator


class Latin(Validator):
    def validate_type(self):
        value = self.data_value.replace(" ", "")
        if value.isalpha():
            return {"status": self.SUCCESS}

        return {
            "status": self.FAILURE,
            "message": "Non-alphabetic characters detected.",
        }

    def validate_format(self):
        if self.data_value == self.data_value.capitalize():
            return {"status": self.SUCCESS}
        return {
            "status": self.WARNING,
            "message": "Genus should be capitalized and species should be lowercased.",
        }
