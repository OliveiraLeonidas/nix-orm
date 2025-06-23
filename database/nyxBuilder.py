from typing import List, Any, Union
from database.parser import SelectNode, insertNode

class NixQuery:
    def __init__(self, orm_instance, query_type: str, table: str, columns: List[str]):
        self.orm = orm_instance
        self.query_type = query_type
        self.table = table
        self.columns = columns
        self._where_conditions = []
        self._limit_value = None
        self._values = {}
    
    def where(self, column: str, operator: str, value: Any):
        self._where_conditions.append({
            'ID': column,
            'EQUALS': operator, 
            'NUMBER': value
        })
        return self
    
    def limit(self, count: int):
        """Adiciona LIMIT"""
        self._limit_value = count
        return self
    
    def values(self, **kwargs):
        self._values.update(kwargs)
        return self
    
    def execute(self):
        node = self._build_node()
        
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Semantic errors: {'; '.join(errors)}")
        
        # Executar
        return self.orm.sql_executor.execute(node, return_sql_only=False)
    
    def sql(self) -> str:
        node = self._build_node()
        
        if not self.orm.semantic_analyzer.analyze(node):
            errors = self.orm.semantic_analyzer.get_errors()
            raise ValueError(f"Semantic errors: {'; '.join(errors)}")
        
        return self.orm.sql_executor.execute(node, return_sql_only=True)
    
    def _build_node(self):
        if self.query_type in ['GET', 'GETALL']:
            node = SelectNode(self.table, self.columns)
            
            if self._where_conditions:
                node.set_where(self._where_conditions[0])
            
            if self._limit_value:
                node.set_limit(self._limit_value)
            
            return node
        
        elif self.query_type == 'INSERT':
            node = insertNode(self.table)
            for col, val in self._values.items():
                node.add_value(col, val)
            return node
        
        else:
            raise ValueError(f"Type Node not supported:  {self.query_type}")