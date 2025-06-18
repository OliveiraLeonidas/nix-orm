from typing import List, Dict, Optional, Union

class NyxBuilder:
    """Construtor de queries com interface fluente para NyxORM"""
    
    def __init__(self, orm):
        self.orm = orm
        self._current_query = None
    
    def create_database(self, name: str) -> 'NyxBuilder':
        self._current_query = f"createDatabase('{name}')"
        return self
    
    def create_table(self, name: str) -> 'NyxTableBuilder':
        return NyxTableBuilder(self.orm, name)
    
    def insert(self, table: str) -> 'NyxInsertBuilder':
        return NyxInsertBuilder(self.orm, table)
    
    def execute(self):
        if not self._current_query:
            raise ValueError("Nenhuma query para executar")
        return self.orm.query(self._current_query)
    
    def sql(self) -> str:
        if not self._current_query:
            raise ValueError("Nenhuma query para converter")
        return self.orm.sql(self._current_query)

class NyxTableBuilder(NyxBuilder):
    """Construtor especializado para CREATE TABLE"""
    
    def __init__(self, orm, table_name: str):
        super().__init__(orm)
        self.table_name = table_name
        self.columns = []
        self._current_query = f"createTable('{table_name}')"
    
    def column(self, name: str, type: str, size: Optional[int] = None, 
               constraints: Optional[List[str]] = None) -> 'NyxTableBuilder':
        constraints = constraints or []
        column_def = {
            'name': name,
            'type': type,
            'size': size,
            'constraints': constraints
        }
        self.columns.append(column_def)
        
        constraints_str = ", " + ", ".join(f"'{c}'" for c in constraints) if constraints else ""
        size_str = f", '{size}'" if size else ""
        self._current_query += f".column('{name}', '{type}'{size_str}{constraints_str})"
        
        return self
    
    def primary_key(self, name: str, type: str = "int") -> 'NyxTableBuilder':
        return self.column(name, type, constraints=['primarykey', 'autoincrement'])
    
    def execute(self):
        result = super().execute()
        self.orm.add_table_schema(self.table_name, [col['name'] for col in self.columns])
        return result

class NyxInsertBuilder(NyxBuilder):
    """Construtor especializado para INSERT"""
    
    def __init__(self, orm, table_name: str):
        super().__init__(orm)
        self.table_name = table_name
        self.values = {} # type: ignore
        self._current_query = f"insert('{table_name}')"
    
    def value(self, column: str, value: Union[str, int, float]) -> 'NyxInsertBuilder':
        self.values[column] = value # type: ignore
        self._current_query += f".values('{column}', '{value}')"
        return self
    
    def values(self, values: Dict[str, Union[str, int, float]]) -> 'NyxInsertBuilder':
        for col, val in values.items():
            self.value(col, val)
        return self