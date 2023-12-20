
from threading import Lock
class Singleton(type):
    _instances = {}
    _lock: Lock = Lock()
    def __call__(cls, *args, **kwargs):
        with cls._lock: 
            if cls not in cls._instances:
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
            return cls._instances[cls]

class Cacher(Metaclass=Singleton):
    def __init__(self):
        self.__dict = dict()
    
    def __getitem__(self, key):
        return self.__dict[key]
    
    def __setitem__(self, key, value): 
        self.__dict[key] = value
    
    def get(self, key):
        return self.__dict.get(key)

    def __contains__(self, key):
        return key in self.__dict

class EngineCacher(Cacher):
    pass 
    
class SessionCacher(Cacher):
    pass


engine_cacher = EngineCacher()
session_cacher = SessionCacher() 


if __name__ == "__main__":
    engine_cacher["a"] = "b"
    print(engine_cacher.get("a"))
    engine_cacher["a"] = "c"


