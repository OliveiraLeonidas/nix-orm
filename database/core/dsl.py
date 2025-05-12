from .lexer import NixLexer
from parser import DSLParser
from semantic import DSLSemanticAnalyzer
from compiler import SQLExecutor

class ORMDSL:
    def __init__(self, connection_string):
        """Inicializa o ORM DSL com a string de conexão do banco de dados"""
        self.lexer = NixLexer()
        self.parser = DSLParser()
        self.executor = SQLExecutor(connection_string)
        self.semantic_analyzer = DSLSemanticAnalyzer(self.executor.engine)
        
    def execute(self, dsl_query):
        """Executa uma consulta DSL e retorna o resultado"""
        # Análise léxica
        tokens = self.lexer.tokenize(dsl_query)
        
        # Análise sintática
        ast = self.parser.parse(dsl_query)
        
        # Análise semântica
        self.semantic_analyzer.load_tables()
        ast = self.semantic_analyzer.analyze(ast)
        
        # Execução
        result = self.executor.execute(ast)
        
        return result
        
    def get_sql(self, dsl_query):
        """Retorna o SQL equivalente para uma consulta DSL (para depuração)"""
        # Análise léxica
        tokens = self.lexer.tokenize(dsl_query)
        
        # Análise sintática
        ast = self.parser.parse(dsl_query)
        
        # Converte para SQL sem executar
        # Este é um método simplificado que precisaria ser implementado
        # no executor para retornar a string SQL em vez de executá-la
        return self.executor.to_sql(ast)
        
    def close(self):
        """Fecha conexões com o banco de dados"""
        self.executor.disconnect()

# Interface de função para uso mais direto
def getAll(table_name):
    """Início de uma construção de consulta SELECT * FROM table"""
    return f"getAll('{table_name}')"

def get(table_name, *columns):
    """Início de uma construção de consulta SELECT col1, col2, ... FROM table"""
    cols = ', '.join([f"'{col}'" for col in columns])
    return f"get('{table_name}', {cols})"
