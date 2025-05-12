# from sqlalchemy import MetaData, String, Table, Integer, Column
# from database.db_engine import EngineDB
# import urllib.parse

# sqlite = "sqlite:///./database/main.db"
# pgdb = "postgresql+pg8000://docker:docker@localhost:5432/docker"

# db = EngineDB(pgdb, True)

# conn = db.engine.connect()

# metadata = MetaData()

# user = Table(
#     'users', metadata,
#     Column('id', Integer, primary_key=True),
#     Column('name', String),
#     Column('email', String, unique=True)
# )

# metadata.create_all(db.engine)

from database.core.dsl import ORMDSL, getAll

# Inicializa o ORM DSL
orm = ORMDSL('sqlite:///example.db')

# Exemplo 1: SELECT * FROM users
query1 = getAll('users')
result1 = orm.execute(query1)
print("Todos os usuários:", result1)
print("SQL:", orm.get_sql(query1))