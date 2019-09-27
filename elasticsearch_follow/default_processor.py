class DefaultProcessor:
    def process_line(self, line):
        entries = [line[key] for key in sorted(line.keys())]
        return ' '.join(entries)
