from typing import Dict, List

from database.compiler import SQLExecutor
from database.nyxBuilder import NixQuery
from database.parser import NixParser
from database.semanticAnalyzer import SemanticAnalyzer


class NixORM:
    """
    pesquisa com base em strings [PARSER]
    Uso:
    db = NixORM()
    
    # Usando strings
    result = db.query("get('users', 'id', 'name')")
    result = db.query("getAll('users').where('id', '=', '1').limit('10')")
    
    # Usando métodos (interface fluente)
    result = db.get('users', 'id', 'name')
    result = db.getAll('users').where('id', '=', '1').limit(10)
    """
    
    def __init__(self, db_connection=None, schema: Dict[str, List[str]] = None):
        self.db_connection = db_connection
        self.schema = schema or {}
        self.parser = NixParser()
        self.semantic_analyzer = SemanticAnalyzer(schema)
        self.sql_executor = SQLExecutor(db_connection)
        self._debug = False
    
    def set_debug(self, debug: bool = True):
        """Ativa/desativa debug"""
        self._debug = debug
        return self
    
    def add_table_schema(self, table: str, columns: List[str]):
        """Adiciona schema de tabela"""
        self.schema[table] = columns
        self.semantic_analyzer.schema[table] = columns
        return self
    
    # ==================== INTERFACE STRING  ====================
    
    def query(self, query_string: str):
        """
        Executa uma query usando string [PARSER]
        
        Exemplos:
        db.query("get('users', 'id', 'name')")
        db.query("getAll('users').where('id', '=', '1')")
        """
        
        if self._debug:
            print(f"[DEBUG] Parsing: {query_string}")
        
        try:
            ast_node = self.parser.parse(query_string)
        except Exception as e:
            raise SyntaxError(f"Erro de parsing: {e}")
        
        if not self.semantic_analyzer.analyze(ast_node):
            errors = self.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        # Executar
        return self.sql_executor.execute(ast_node, return_sql_only=False)
    
    def sql(self, query_string: str) -> str:
        """
        Retorna apenas o SQL sem executar
        
        Exemplo:
        sql = db.sql("get('users').where('id', '=', '1')")
        """
        
        ast_node = self.parser.parse(query_string)
        
        if not self.semantic_analyzer.analyze(ast_node):
            errors = self.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        return self.sql_executor.execute(ast_node, return_sql_only=True) # type: ignore
    
    # ==================== INTERFACE ====================
    
    def get(self, table: str, *columns):
        """Interface fluente para GET"""
        return NixQuery(self, 'GET', table, list(columns) if columns else ['*'])
    
    def getAll(self, table: str):
        """Interface fluente para GETALL"""
        return NixQuery(self, 'GETALL', table, ['*'])
    
    def insert(self, table_name, col, values):
        return 


db = NixORM()
result = db.query("get('users', 'id', 'name', 'age')")
result = db.query("getAll('users').where('id', '=', '10')")