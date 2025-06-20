from typing import Dict, List

from database.compiler import SQLExecutor
from database.nyxBuilder import NixQuery
from database.parser import NixParser
from database.semanticAnalyzer import SemanticAnalyzer


class NixORM:
    """
    Uso:
    db = NixORM()
    
    # Usando strings
    result = db.query("get('users', 'id', 'name')")
    result = db.query("getAll('users').where('id', '=', '1').limit('10')")
    
    # Usando métodos (interface)
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
        Usando
        
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
    
    def insert(self, table_name: str):
        """Interface fluente para INSERT"""
        return NixQuery(self, 'INSERT', table_name, [])

    def createTable(self, table_name: str):
        """Cria uma nova tabela usando interface fluente"""
        return TableBuilder(self, table_name)

    def createDatabase(self, database_name: str):
        """Cria um novo banco de dados"""
        query_string = f"createDatabase('{database_name}')"
        return self.query(query_string)

    def get_schema(self):
        """Retorna o schema atual"""
        return self.semantic_analyzer.get_schema()

    def get_last_sql(self):
        """Retorna o último SQL executado"""
        return self.sql_executor.get_last_sql()

class TableBuilder:
    """Builder para CREATE TABLE com interface fluente"""
    
    def __init__(self, orm_instance, table_name: str):
        self.orm = orm_instance
        self.table_name = table_name
        self._columns = []
    
    def column(self, name: str, data_type: str, *constraints):
        """Adiciona uma coluna à tabela"""
        column_def = {
            'name': name,
            'type': data_type,
            'constraints': []
        }
        
        for constraint in constraints:
            if isinstance(constraint, str):
                if constraint.isdigit():
                    column_def['size'] = int(constraint)
                else:
                    column_def['constraints'].append(constraint.lower())
        
        self._columns.append(column_def)
        return self
    
    def primaryKey(self, column_name: str, data_type: str = 'INTEGER'):
        """Adiciona coluna primary key"""
        return self.column(column_name, data_type, 'primarykey', 'autoincrement')
    
    def execute(self):
        """Executa a criação da tabela"""
        from database.parser import createTableNode
        
        node = createTableNode(self.table_name)
        for col in self._columns:
            node.add_column(col)
        
        # Análise semântica
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        return self.orm.sql_executor.execute(node, return_sql_only=False)
    
    def sql(self) -> str:
        """Retorna apenas o SQL da criação da tabela"""
        from database.parser import createTableNode
        
        node = createTableNode(self.table_name)
        for col in self._columns:
            node.add_column(col)
        
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Erros semânticos: {'; '.join(errors)}")
        
        return self.orm.sql_executor.execute(node, return_sql_only=True)



# Exemplo de uso completo:
if __name__ == "__main__":
    # Inicializar ORM
    db = NixORM().set_debug(True)
    
    try:
        # Teste 1: Criar banco de dados
        print("=== Criando Database ===")
        db.createDatabase('ecommerce')
        
        # Teste 2: Criar tabela usando interface fluente
        print("\n=== Criando Tabela ===")
        sql = db.createTable('users')\
               .primaryKey('id')\
               .column('name', 'VARCHAR', '100', 'notnull')\
               .column('email', 'VARCHAR', '255', 'unique')\
               .column('age', 'INTEGER')\
               .sql()
        print(f"SQL: {sql}")
        
        # Teste 3: Insert usando interface fluente
        print("\n=== Insert Fluente ===")
        sql = db.insert('users')\
               .values(name='João', email='joao@test.com', age=25)\
               .sql()
        print(f"SQL: {sql}")
        
        # Teste 4: Query usando strings
        print("\n=== Query String ===")
        sql = db.sql("get('users', 'name', 'email').where('age', '>', '18')")
        print(f"SQL: {sql}")
        
        # Teste 5: Query usando interface fluente
        print("\n=== Query Fluente ===")
        sql = db.get('users', 'name', 'email')\
               .where('age', '>', 18)\
               .limit(10)\
               .sql()
        print(f"SQL: {sql}")
        
        # Teste 6: GetAll
        print("\n=== GetAll ===")
        sql = db.getAll('users')\
               .where('status', '=', 'active')\
               .sql()
        print(f"SQL: {sql}")
        
        # Mostrar schema gerado
        print(f"\n=== Schema Gerado ===")
        print(db.get_schema())
        
    except Exception as e:
        print(f"Erro: {e}")