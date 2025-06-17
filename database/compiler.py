from typing import Dict
from database.parser import CreateDatabaseNode, SelectNode, insertNode, createTableNode

class SQLExecutor:
    """ Analisa o Node recebido e retorna o equivalente em SQL"""
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
        self.last_sql = None
    
    def execute(self, node, return_sql_only: bool = True):
        if isinstance(node, SelectNode):
            sql = self._generate_select_sql(node)
        elif isinstance(node, CreateDatabaseNode):
            sql = self._generate_create_database_sql(node)
        elif isinstance(node, createTableNode):
            sql = self._generate_create_table_sql(node)
        elif isinstance(node, insertNode):
            sql = self._generate_insert_sql(node)
        else:
            raise ValueError(f"Tipo de node não suportado: {type(node)}")
        
        self.last_sql = sql
        
        if return_sql_only:
            return sql
        else:
            # Aqui executaria no banco se tivesse conexão
            return self._execute_sql(sql) if self.db_connection else sql
    
    def _generate_create_database_sql(self, node: CreateDatabaseNode) -> str:
        if not node.database_name:
            raise ValueError("Nome do banco de dados não pode estar vazio")
        
        return f"CREATE DATABASE `{node.database_name}`;"
    
    def _generate_create_table_sql(self, node: createTableNode) -> str:
        """Gera SQL para CREATE TABLE"""
        if not node.table_name:
            raise ValueError("Nome da tabela não pode estar vazio")
        
        if not node.columns:
            raise ValueError("Tabela deve ter pelo menos uma coluna")
        
        columns_sql = []
        for col in node.columns:
            col_sql = f"`{col['name']}` {col['type']}"
            
            # Adiciona tamanho se especificado
            if 'size' in col:
                col_sql += f"({col['size']})"
            
            # Adiciona constraints
            if 'constraints' in col:
                for constraint in col['constraints']:
                    if constraint == 'primarykey':
                        col_sql += " PRIMARY KEY"
                    elif constraint == 'notnull':
                        col_sql += " NOT NULL"
                    elif constraint == 'unique':
                        col_sql += " UNIQUE"
                    elif constraint == 'autoincrement':
                        col_sql += " AUTO_INCREMENT"
            
            columns_sql.append(col_sql)
        
        return f"CREATE TABLE `{node.table_name}` ({', '.join(columns_sql)});"
    
    def _generate_insert_sql(self, node: insertNode) -> str:
        """Gera SQL para INSERT"""
        if not node.table_name:
            raise ValueError("Nome da tabela não pode estar vazio")
        
        if not node.values:
            raise ValueError("Insert deve ter pelo menos um valor")
        
        columns = list(node.values.keys())
        values = list(node.values.values())
        
        columns_str = ', '.join(f"`{col}`" for col in columns)
        values_str = ', '.join(f"'{val}'" for val in values)
        
        return f"INSERT INTO `{node.table_name}` ({columns_str}) VALUES ({values_str});"
    
    def _generate_select_sql(self, node: SelectNode) -> str:
        """Gera SQL para SELECT"""
        if node.columns == ['*']:
            columns_str = '*'
        else:
            columns_str = ', '.join(f"`{col}`" for col in node.columns)
        
        sql = f"SELECT {columns_str} FROM `{node.table}`"
        
        if node.where:
            where_clause = self._generate_where_clause(node.where)
            sql += f" WHERE {where_clause}"
        
        if node.limit:
            sql += f" LIMIT {int(node.limit)}"
        
        return sql + ";"
    
    def _generate_where_clause(self, condition: Dict) -> str:
        """Gera cláusula WHERE"""
        column = condition.get('ID')
        operator = condition.get('EQUALS')
        value = condition.get('NUMBER')
        
        # Formatar valor
        try:
            if isinstance(value, str) and value.isdigit():
                formatted_value = value
            elif isinstance(value, (int, float)):
                formatted_value = str(value)
            else:
                formatted_value = f"'{value}'"
        except (ValueError, TypeError):
            formatted_value = f"'{value}'"
        
        return f"`{column}` {operator} {formatted_value}"
    
    def _execute_sql(self, sql: str):
        """Executa SQL no banco (se houver conexão)"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            
            # Converter para dicionários
            column_names = [desc[0] for desc in cursor.description]
            return [dict(zip(column_names, row)) for row in results]
            
        except Exception as e:
            raise RuntimeError(f"Erro ao executar SQL: {e}")
    
    def get_last_sql(self):
        return self.last_sql


# Teste do executor
if __name__ == "__main__":
    from parser import NixParser
    
    parser = NixParser()
    executor = SQLExecutor()
    
    test_queries = [
        "createDatabase('eCom')",
        "createTable('users').column('id', 'INTEGER', 'primarykey').column('name', 'VARCHAR', '100')",
        "get('users', 'name').where('age', '>', '18')",
        "getAll('users')",
        "insert('users').values('name', 'John', 'age', '25')"
    ]
    
    for query in test_queries:
        try:
            print(f"\n=== Testando: {query} ===")
            result = parser.parse(query)
            print(f"Parsed: {result}")
            
            sql = executor.execute(result)
            print(f"SQL gerado: {sql}")
                
        except Exception as e:
            print(f"Erro: {e}")