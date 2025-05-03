from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class EngineDB:
    def __init__(self,  url: str, echo: bool = False):
        self.url = url
        self.echo = echo
        self.engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        # its a named argument
        return create_engine(self.url, echo=self.echo) # type: ignore