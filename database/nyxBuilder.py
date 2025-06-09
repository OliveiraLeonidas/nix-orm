from typing import List, TYPE_CHECKING


if TYPE_CHECKING:
    from database.nyx import NixORM 


class NixQuery:
    """Query builder que gera strings compatíveis com seu parser"""
    
    def __init__(self, orm: 'NixORM', operation: str, table: str, columns: List[str]):
        self.orm = orm
        self.operation = operation
        self.table = table
        self.columns = columns
        self._where_condition = None
        self._limit_value = None
    
    def where(self, column: str, operator: str, value: str):
        self._where_condition = (column, operator, value)
        return self
    
    def limit(self, limit_value: int):
        self._limit_value = limit_value
        return self
    
    def execute(self):
        """Executa a query"""
        query_string = self._build_query_string()
        return self.orm.query(query_string)
    
    def sql(self) -> str:
        """Retorna SQL"""
        query_string = self._build_query_string()
        return self.orm.sql(query_string)
    
    def _build_query_string(self) -> str:
        """Constrói string compatível com seu parser"""
        
        if self.operation == 'GETALL':
            query = f"getAll('{self.table}')"
        else: 
            if self.columns == ['*']:
                query = f"get('{self.table}')"
            else:
                columns_str = "', '".join(self.columns)
                query = f"get('{self.table}', '{columns_str}')"
        
        if self._where_condition:
            col, op, val = self._where_condition
            query += f".where('{col}', '{op}', '{val}')"
        
        if self._limit_value:
            query += f".limit('{self._limit_value}')"
        
        return query
    
    def __repr__(self):
        return f"<NixQuery: {self._build_query_string()}>"