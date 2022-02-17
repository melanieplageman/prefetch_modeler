import collections.abc

registry = {}


class overrideable:
    def __init__(self, name):
        self.name = name

    def __call__(self, function):
        def overrideable_function(*args, **kwargs):
            override = registry.get(self.name, None)
            if override is not None:
                return override(*args, **kwargs)
            return function(*args, **kwargs)
        return overrideable_function


class override:
    def __init__(self, function_to_override):
        self.function_to_override = function_to_override

    def __call__(self, function):
        registry[self.function_to_override] = function
        return function


class AlgorithmCollection(collections.abc.Set):
    def __init__(self):
        self.registry = set()

    def __contains__(self, item):
        return item in self.registry

    def __iter__(self):
        return iter(self.registry)

    def __len__(self):
        return len(self.registry)

    def algorithm(self, function):
        self.registry.add(function)
