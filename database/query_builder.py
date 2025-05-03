# Construindo sem o SQLAlchemy Core
import pg8000, sqlite3, os

class NixQueryBuilder:

    def __init__(self, table_name):
        self.table_name = table_name
        self.columns = []
        self.conditions = []
        self.joins = []
        self.orderBy = None
        self.limitValue = None
        self.offsetValue = None
        self.aliases = {}
        self.parameters = []
    
    def where(self, column, operator, value):
        self.conditions.append((column, operator, value))
        self.parameters.append(value)
        return self

    def whereIn(self, column, values):
        placeholders = ", ".join(["?"]* len(values))
        self.conditions.append((column, "IN", f"({placeholders})"))
        self.parameters.extend(values)
        return self



class Connection:
    def __init__(self, db_type, **kwargs):
        self.db_type = db_type.lower()
        self.conn = None
        self.connection_params = kwargs


        if self.db_type == '':
            self.connect_sqlite()


    def connect_sqlite(self):
        return