

class PipeSegment:
    def __init__(self, impl):
        self._impl = impl

    def to_verification_string(self):
        return f'+--{self._impl.name}'
