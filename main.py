from sqlalchemy import MetaData, String, Table, Integer, Column
from database.db_engine import EngineDB
import urllib.parse

sqlite = "sqlite:///./database/main.db"
pgdb = "postgresql+pg8000://docker:docker@localhost:5432/docker"

db = EngineDB(pgdb, True)

conn = db.engine.connect()

metadata = MetaData()

user = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('email', String, unique=True)
)

metadata.create_all(db.engine)
