from typing import Dict
from database.parser import SelectNode

class SQLExecutor:
    """Executor que converte Node em SQL e executa"""
    
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
        self.last_sql = None
    
    def execute(self, node: SelectNode, return_sql_only: bool = False):
        """Executa um SelectNode"""

        if isinstance(node, SelectNode):
            pass
        
        # if not isinstance(node, SelectNode):
        #     raise ValueError(f"Tipo de nó não suportado: {type(node)}")
        
        # sql = self._generate_select_sql(node)
        # self.last_sql = sql
        
        # if return_sql_only:
        #     return sql
        
        # if self.db_connection:
        #     return self._execute_sql(sql)
        # else:
        #     print(f"SQL Gerado: {sql}")
        #     return sql
    
    def _generate_select_sql(self, node: SelectNode) -> str:
        """Gera SQL para um SelectNode"""
        
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
        
        return sql
    
    def _generate_where_clause(self, condition: Dict) -> str:
        """Gera cláusula WHERE"""
        column = condition.get('ID')
        operator = condition.get('EQUALS')
        value = condition.get('NUMBER')
        
        # Formatar valor
        try:
            if value is not None and '.' in str(value):
                float(value)
                formatted_value = value
            elif value is not None:
                int(value)
                formatted_value = value
            else:
                raise ValueError("Value is None")
        except (ValueError, TypeError):
            formatted_value = f"'{value}'"
        
        return f"`{column}` {operator} {formatted_value}"
    
    def _execute_sql(self, sql: str):
        """Executa SQL no banco"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            
            # Converter para dicionários
            column_names = [desc[0] for desc in cursor.description]
            return [dict(zip(column_names, row)) for row in results]
            
        except Exception as e:
            raise RuntimeError(f"Erro ao executar SQL: {e}")
    
    def get_last_sql(self) -> str:
        return self.last_sql # type: ignore