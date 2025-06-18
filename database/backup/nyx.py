from typing import Dict, List, Optional
from database.compiler import SQLExecutor
from database.parser import NixParser
from database.semanticAnalyzer import SemanticAnalyzer

class NyxORM:
    """
    ORM principal com duas interfaces:
    1. Direta com strings no formato Nix
    2. Fluente através do NyxBuilder
    """
    
    def __init__(self, db_connection=None, schema: Dict[str, List[str]] = None):
        self.db_connection = db_connection
        self.schema = schema or {}
        self.parser = NixParser()
        self.semantic_analyzer = SemanticAnalyzer(schema)
        self.sql_executor = SQLExecutor(db_connection) # type: ignore
        self._debug = False

        # Verifica se é uma conexão SQLite3 direta
        if db_connection and hasattr(db_connection, 'cursor'):  # É uma conexão SQLite3
            # Cria uma URL SQLite em memória para o SQLAlchemy
            self.sql_executor = SQLExecutor("sqlite:///:memory:")
            self.sql_executor.engine = db_connection  # Substitui o engine pelo connection
        else:
            self.sql_executor = SQLExecutor(db_connection)  # type: ignore # Usa normalmente para strings

    
    def set_debug(self, debug: bool = True):
        self._debug = debug
        return self
    
    def add_table_schema(self, table: str, columns: List[str]):
        self.schema[table] = columns
        self.semantic_analyzer.schema[table] = columns
        return self
    
    # Interface string/Nix
    def query(self, query_string: str):
        if self._debug:
            print(f"[DEBUG] Parsing: {query_string}")
        
        try:
            ast_node = self.parser.parse(query_string)
        except Exception as e:
            raise SyntaxError(f"Erro de parsing: {e}")
        
        if not self.semantic_analyzer.analyze(ast_node):
            errors = self.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        return self.sql_executor.execute(ast_node, return_sql_only=False)
    
    def sql(self, query_string: str) -> str:
        ast_node = self.parser.parse(query_string)
        
        if not self.semantic_analyzer.analyze(ast_node):
            errors = self.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        return self.sql_executor.execute(ast_node, return_sql_only=True) # type: ignore
    
    # Interface fluente
    def get(self, table: str, *columns):
        return self.NyxQuery(self, 'GET', table, list(columns) if columns else ['*'])
    
    def getAll(self, table: str):
        return self.NyxQuery(self, 'GETALL', table, ['*'])
    
    def builder(self):
        from database.nyxBuilder import NyxBuilder
        return NyxBuilder(self)

    class NyxQuery:
        """Representa uma query em construção"""
        
        def __init__(self, orm, operation: str, table: str, columns: List[str]):
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
            return self.orm.query(self._build_query_string())
        
        def sql(self) -> str:
            return self.orm.sql(self._build_query_string())
        
        def _build_query_string(self) -> str:
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
            return f"<NyxQuery: {self._build_query_string()}>"