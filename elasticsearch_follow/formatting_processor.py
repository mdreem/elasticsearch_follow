import string


class DefaultValueFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        try:
            val = super(DefaultValueFormatter, self).get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            val = '', field_name
        return val


class FormattingProcessor:
    def __init__(self, format_string):
        self.fmt = DefaultValueFormatter()
        self.format_string = format_string

    def process_line(self, line):
        if self.format_string:
            return self.fmt.format(self.format_string, **line)
