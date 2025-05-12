from sqlalchemy import create_engine, text, MetaData, Table, Column, select, insert, update, delete
from sqlalchemy.sql import and_, or_, desc, asc
# from .utils.errors import ExecutionError

class SQLExecutor:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()
        self.connection = None
        
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        if not self.connection:
            self.connection = self.engine.connect()
        return self.connection
        
    def disconnect(self):
        """Fecha a conexão com o banco de dados"""
        if self.connection:
            self.connection.close()
            self.connection = None
            
    def execute(self, ast):
        """Executa a consulta a partir da AST"""
        if not ast:
            return None
            
        conn = self.connect()
        query_type = ast.get('type')
        
        try:
            if query_type == 'select':
                return self._execute_select(ast, conn)
            elif query_type == 'insert':
                return self._execute_insert(ast, conn)
            elif query_type == 'update':
                return self._execute_update(ast, conn)
            elif query_type == 'delete':
                return self._execute_delete(ast, conn)
            else:
                raise Exception(f"Tipo de consulta desconhecido: {query_type}")
        except Exception as e:
            raise Exception(f"Erro ao executar consulta: {str(e)}")
    
    def _execute_select(self, ast, conn):
        """Executa uma consulta SELECT"""
        # Carrega a tabela da metadado
        table_name = ast['table']
        if table_name not in self.metadata.tables:
            self.metadata.reflect(self.engine, only=[table_name])
        
        table = self.metadata.tables[table_name]
        
        # Constrói a consulta select
        if '*' in ast['columns']:
            query = select(table)
        else:
            columns = [getattr(table.c, col) for col in ast['columns'] if '.' not in col]
            # TODO: Lidar com colunas com alias
            query = select(*columns)
            