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
        self._debug = debug
        return self
    
    def add_table_schema(self, table: str, columns: List[str]):
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
            raise ValueError(f"Semantic errors {'; '.join(errors)}")
        
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
            raise ValueError(f"Semantic errors {'; '.join(errors)}")
        
        return self.sql_executor.execute(ast_node, return_sql_only=True) # type: ignore
    
    # ==================== INTERFACE ====================
    
    def get(self, table: str, *columns):
        return NixQuery(self, 'GET', table, list(columns) if columns else ['*'])
    
    def getAll(self, table: str):
        return NixQuery(self, 'GETALL', table, ['*'])
    
    def insert(self, table_name: str):
        return NixQuery(self, 'INSERT', table_name, [])

    def createTable(self, table_name: str):
        return TableBuilder(self, table_name)

    def createDatabase(self, database_name: str):
        query_string = f"createDatabase('{database_name}')"
        return self.query(query_string)

    def get_schema(self):
        return self.semantic_analyzer.get_schema()

    def get_last_sql(self):
        return self.sql_executor.get_last_sql()

class TableBuilder:
    def __init__(self, orm_instance, table_name: str):
        self.orm = orm_instance
        self.table_name = table_name
        self._columns = []
    
    def column(self, name: str, data_type: str, *constraints):
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
        from database.parser import createTableNode
        
        node = createTableNode(self.table_name)
        for col in self._columns:
            node.add_column(col)
        
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Semantic errors {'; '.join(errors)}")
        
        return self.orm.sql_executor.execute(node, return_sql_only=False)
    
    def sql(self) -> str:
        from database.parser import createTableNode
        
        node = createTableNode(self.table_name)
        for col in self._columns:
            node.add_column(col)
        
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Semantic errors {'; '.join(errors)}")
        
        return self.orm.sql_executor.execute(node, return_sql_only=True)



if __name__ == "__main__":
    # Inicializar ORM
    db = NixORM().set_debug(True)
    
    try:
        print("=== Criando Database ===")
        db.createDatabase('ecommerce')

        print("\n=== Criando Tabela ===")
        sql = db.createTable('users')\
               .primaryKey('id')\
               .column('name', 'VARCHAR', '100', 'notnull')\
               .column('email', 'VARCHAR', '255', 'unique')\
               .column('age', 'INTEGER')\
               .sql()
        print(f"SQL: {sql}")
        
        print("\n=== Insert Fluente ===")
        sql = db.insert('users')\
               .values(name='João', email='joao@test.com', age=25)\
               .sql()
        print(f"SQL: {sql}")
        
        print("\n=== Query String ===")
        sql = db.sql("get('users', 'name', 'email').where('age', '>', '18')")
        print(f"SQL: {sql}")
        
        print("\n=== Query Fluente ===")
        sql = db.get('users', 'name', 'email')\
               .where('age', '>', 18)\
               .limit(10)\
               .sql()
        print(f"SQL: {sql}")
        
        print("\n=== GetAll ===")
        sql = db.getAll('users')\
               .where('status', '=', 'active')\
               .sql()
        print(f"SQL: {sql}")
        
        print(f"\n=== Schema Gerado ===")
        print(db.get_schema())
        
    except Exception as e:
        print(f"Erro: {e}")