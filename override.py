import functools

class Overrideable:
    def __init__(self):
        self.override = {}


def overrideable(function):
    @functools.wraps(function)
    def overrideable_function(self, *args, **kwargs):
        override = self.override.get(function.__name__, None)
        if override is not None:
            return override(self, function, *args, **kwargs)
        return function(self, *args, **kwargs)
    return overrideable_function

