from sqlalchemy import MetaData, Table, inspect

class NixSemanticAnalyzer:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()
        self.inspector = inspect(engine)
        
    def load_tables(self):
        """Carrega todas as tabelas do banco de dados para metadados"""
        self.metadata.reflect(bind=self.engine)
        
    def analyze(self, ast):
        """Analisa a AST em busca de erros semânticos"""
        if not ast:
            return ast
            
        query_type = ast.get('type')
        
        if query_type == 'select':
            return self._analyze_select(ast)
        elif query_type == 'insert':
            return self._analyze_insert(ast)
        elif query_type == 'update':
            return self._analyze_update(ast)
        elif query_type == 'delete':
            return self._analyze_delete(ast)
        else:
            raise Exception(f"Tipo de consulta desconhecido: {query_type}")