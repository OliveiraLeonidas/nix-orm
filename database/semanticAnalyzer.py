from typing import Dict, List
from parser import NixParser, SelectNode, insertNode

class SemanticAnalyzer:
    """Analisador semântico que valida a árvore sintática"""
    
    def __init__(self, schema: Dict[str, List[str]] = None): # type: ignore
        self.schema = schema or {}
        self.errors = []
        self.warnings = []
    
    def analyze(self, node: SelectNode) -> bool:
        """Analisa um SelectNode"""
        self.errors = []
        self.warnings = []
        
        
        if isinstance(node, insertNode):
            self._analyze_insert_node(node)
        elif isinstance(node, SelectNode):
            self._analyze_select_node(node)
        else:
            self.errors.append(f"Tipo de nó não suportado: {type(node)}")
        
        return len(self.errors) == 0
    
    def _analyze_insert_node(self, node):
        
        if not node.table_name:
            self.errors.append("Nome da tabela não pode estar vazio")
            return
        
        if not node.values:
            self.errors.append("Insert deve ter pelo menos um valor")
            return
        
        if self.schema and node.table_name not in self.schema:
            self.errors.append(f"Tabela '{node.table_name}' não encontrada")
            return
        
        if self.schema and node.table_name in self.schema:
            available_columns = self.schema[node.table_name]
            for col in node.values.keys():
                if col not in available_columns:
                    self.errors.append(f'Coluna \'{col}\' não encontrada na tabela \'{node.table_name}\'')
    
    def _analyze_select_node(self, node: SelectNode):
        """Node SELECT => COMANDO"""
        
        if self.schema and node.table not in self.schema:
            self.errors.append(f"Tabela '{node.table}' não encontrada no schema")
            return
        
        if node.columns != ['*'] and self.schema:
            available_columns = self.schema.get(node.table, [])
            for column in node.columns:
                if column not in available_columns:
                    self.errors.append(f"Coluna '{column}' não encontrada na tabela '{node.table}'")
        
        if node.where:
            self._analyze_where_condition(node.where, node.table)
        
        if node.limit:
            try:
                limit_value = int(node.limit)
                if limit_value <= 0:
                    self.errors.append("LIMIT deve ser um número positivo")
            except ValueError:
                self.errors.append("LIMIT deve ser um número válido")
    
    def _analyze_where_condition(self, condition: Dict, table: str):
        """Analisa condições WHERE"""
        column = condition.get('ID')
        operator = condition.get('EQUALS')
        value = condition.get('NUMBER')
        
        if not all([column, operator, value]):
            self.errors.append("Condição WHERE incompleta")
            return
        
        if self.schema and table in self.schema:
            if column not in self.schema[table]:
                self.errors.append(f"Coluna '{column}' não encontrada na tabela '{table}'")
        
        valid_operators = ['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN']
        if operator not in valid_operators:
            self.errors.append(f"Operador '{operator}' não suportado")
    
    def get_errors(self) -> List[str]:
        return self.errors
    
    def get_warnings(self) -> List[str]:
        return self.warnings


# Teste do analisador semântico
if __name__ == "__main__":
    # Schema de exemplo
    schema = {
        'users': ['id', 'name', 'age', 'email']
    }
    
    # Criar parser e analisador
    parser = NixParser()
    analyzer = SemanticAnalyzer(schema)
    
    # Testar diferentes consultas
    test_queries = [
        "get('users', 'name').where('age', '>', '18')",  # Válida
        "get('invalid_table', 'name')",                   # Tabela inválida
        "get('users', 'invalid_column')",                 # Coluna inválida
        "get('users', 'name').where('invalid_col', '=', '1')",  # Coluna WHERE inválida
    ]
    
    for query in test_queries:
        try:
            print(f"\n=== Testando: {query} ===")
            result = parser.parse(query)
            print(f"Parsed: {result}")
            
            is_valid = analyzer.analyze(result)
            print(f"Válido: {is_valid}")
            
            if analyzer.get_errors():
                print(f"Erros: {analyzer.get_errors()}")
            
            if analyzer.get_warnings():
                print(f"Avisos: {analyzer.get_warnings()}")
                
        except Exception as e:
            print(f"Erro no parsing: {e}")