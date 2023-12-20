from abc import ABC, abstractmethod 



class Storer(ABC):
    def __init__(self, session=None):
        self._session = session 

    @property
    def session(self):
        return self._session
    @session.setter
    def session(self, session):
        self._session = session
    @abstractmethod
    def check_database(self):
        pass
    @abstractmethod
    def chunk(self):
        pass
    @abstractmethod
    def persist(self):
        pass
    @abstractmethod
    def iter_data(self):
        pass
    @abstractmethod
    def store(self, data):
        pass



class DfStorer(ABC): 
    @abstractmethod
    def check_db(self):
        pass
    @abstractmethod
    def chunk(self):
        pass
    @abstractmethod
    def iter_data(self):
        pass
    @abstractmethod
    def store(self, data):
        pass
        
