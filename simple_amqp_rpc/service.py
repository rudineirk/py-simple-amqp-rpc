class Service:
    def __init__(self, name):
        self.name = name
        self.methods = {}

    def rpc(self, func_name):
        def decorator(func):
            self._save_method(func_name, func)
            return func

        if isinstance(func_name, str):
            return decorator

        func = func_name
        func_name = func.__name__
        return decorator(func)

    def _save_method(self, method: str, func):
        self.methods[method] = func.__name__

    def get_methods(self, obj):
        methods = {}
        for method, func_name in self.methods.items():
            func = getattr(obj, func_name)
            methods[method] = func

        return methods
