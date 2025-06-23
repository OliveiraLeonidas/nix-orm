from typing import Dict, List
from database.parser import CreateDatabaseNode, NixParser, SelectNode, insertNode, createTableNode

class SemanticAnalyzer:
    
    def __init__(self, schema: Dict[str, List[str]] = None):
        self.schema = schema or {}
        self.errors = []
        self.warnings = []
    
    def analyze(self, node) -> bool:
        self.errors = []
        self.warnings = []
        
        if isinstance(node, CreateDatabaseNode):
            self._analyze_create_database(node)
        elif isinstance(node, createTableNode):
            self._analyze_create_table(node)
        elif isinstance(node, insertNode):
            self._analyze_insert_node(node)
        elif isinstance(node, SelectNode):
            self._analyze_select_node(node)
        else:
            self.errors.append(f"Type Node not supported: {type(node)}")
        
        return len(self.errors) == 0
    
    def _analyze_create_database(self, node):
        if not node.database_name:
            self.errors.append("Database name is empty!")
            return
        
        # Atualiza o schema com o nome do banco
        self.schema["_database_name"] = node.database_name
    
    def _analyze_create_table(self, node):
        if not node.table_name:
            self.errors.append("Table name is empty")
            return
        
        if not node.columns:
            self.errors.append("Table should have at least one column")
            return
        
        # Gera o schema da tabela automaticamente
        column_names = [col['name'] for col in node.columns]
        self.schema[node.table_name] = column_names
    
    def _analyze_insert_node(self, node):
        if not node.table_name:
            self.errors.append("Nome da tabela não pode estar vazio")
            return
        
        if not node.values:
            self.errors.append("Insert deve ter pelo menos um valor")
            return
        
        if node.table_name not in self.schema:
            self.warnings.append(f"Table '{node.table_name}' not found on schema, it will be created")
            self.schema[node.table_name] = list(node.values.keys())
        else:
            # Valida colunas existentes
            available_columns = self.schema[node.table_name]
            for col in node.values.keys():
                if col not in available_columns:
                    self.errors.append(f"Column'{col}' not found on table '{node.table_name}'")
    
    def _analyze_select_node(self, node: SelectNode):
        # Se a tabela não existe no schema, cria com colunas genéricas
        if node.table not in self.schema:
            self.warnings.append(f"Table '{node.table}' not found on schema, creating basic structure")
            # Cria schema básico baseado nas colunas solicitadas
            if node.columns != ['*']:
                self.schema[node.table] = node.columns
            else:
                self.schema[node.table] = ['id']  # Coluna padrão
        
        if node.columns != ['*'] and self.schema:
            available_columns = self.schema.get(node.table, [])
            for column in node.columns:
                if column not in available_columns:
                    self.errors.append(f"Column '{column}' not found on table '{node.table}'")
        
        if node.where:
            self._analyze_where_condition(node.where, node.table)
        
        if node.limit:
            try:
                limit_value = int(node.limit)
                if limit_value <= 0:
                    self.errors.append("LIMIT should be greater than 0")
            except ValueError:
                self.errors.append("LIMIT should be a valid number")
    
    def _analyze_where_condition(self, condition: Dict, table: str):
        column = condition.get('ID')
        operator = condition.get('EQUALS')
        value = condition.get('NUMBER')
        
        if not all([column is not None, operator, value is not None]):
            self.errors.append("WHERE contitions is not complete")
            return
        
        if self.schema and table in self.schema:
            if column not in self.schema[table]:
                self.errors.append(f"Column '{column}' not found on table '{table}'")
        
        valid_operators = ['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN']
        if operator not in valid_operators:
            self.errors.append(f"Operator '{operator}' not supported")
    
    def get_errors(self) -> List[str]:
        return self.errors
    
    def get_warnings(self) -> List[str]:
        return self.warnings
    
    def get_schema(self) -> Dict[str, List[str]]:
        return self.schema


# Teste do analisador semântico
if __name__ == "__main__":
    parser = NixParser()
    analyzer = SemanticAnalyzer()
    
    test_queries = [
        "createDatabase('eCom')",
        "createTable('users').column('id', 'INTEGER', 'primarykey').column('name', 'VARCHAR', '100').column('age', 'INTEGER')",
        "get('users', 'name').where('age', '>', '18')",
        "getAll('users')",
        "insert('users').values('name', 'John', 'age', '25')"
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
            
            print(f"Schema atual: {analyzer.get_schema()}")
                
        except Exception as e:
            print(f"Erro no parsing: {e}")