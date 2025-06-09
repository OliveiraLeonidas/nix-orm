from lexer import NixLexer
import sys

# Criando parser com base nos conteúdos de aula, pois fornece um melhor controle sobre o parseamento das classes
# Como ele trabalha a apartir dos tokens definidos a MV vai gerar um sql equivalente para devolver a api que está sendo utilizada.

class Node:
    def toDict(self):
        return self.__dict__

# Criando arvore de símbolos
class SelectNode(Node):
    def __init__(self, table, columns=None):
        self.type = 'SELECT'
        self.table = table
        self.columns = columns or ['*']
        self.where = None
        self.limit = None
    

    def set_where(self, condition):
        self.where = condition
    
    def set_limit(self, limit):
        self.limit = limit
    
    def __repr__(self):
        return f'<Selected Node: table_name={self.table} and columns name {self.columns} where={self.where} limit={self.limit}>'


class createTableNode(Node):
    def __init__(self, table_name):
        self.type = "CREATE TABLE"
        self.table_name = table_name
        self.columns = []

    def add_column(self, column_def):
        self.columns.append(column_def)
    
    def __repr__(self) -> str:
        return f'<CreateTableNode: table={self.table_name} columns={self.columns}>'
        

class insertNode(Node):
    def __init__(self, table_name):
        self.type = 'INSERT'
        self.table_name = table_name
        self.values = {}
    
    def add_value(self, column, value):
        self.values[column] = value
    
    def __repr__(self):
        return f'<InsertNode: table={self.table_name} values={self.values}>'

class NixParser:
    def __init__(self):
        self.lexer = None
        self.lookAhead = None
        self.symbols = []
    
    def parse(self, data):
        self.lexer = NixLexer(data)
        self.lookAhead = self.lexer.nextToken()
        return self.parse_expression()

    def match(self, expected_type):
        if self.lookAhead is None:
            self.error(f'Expect {expected_type}, and end of line found it')
        elif self.lookAhead.type == expected_type:
            current = self.lookAhead
            self.lookAhead = self.lexer.nextToken()
            return current
        else:
            self.error(f'Expect {expected_type}, and found type {self.lookAhead.type}')
    
    def error(self, message):
        print(f'[Sintax error] {message}')
        sys.exit(1)
    
    def parse_expression(self):
        if self.lookAhead.type == 'GETALL':
            return self.parse_getAll()
        elif self.lookAhead.type == 'GET':
            return self.parse_get() 
        
        elif self.lookAhead.type == 'CREATETABLE':
            return self.parse_create_table()
        elif self.lookAhead.type == 'INSERT':
            return self.parse_insert()
        else:
            raise SyntaxError("Unknown expression")
    
    def parse_getAll(self):
        self.match("GETALL")
        self.match("LPAREN")
        tableToken = self.match("STRING").value
        self.match("RPAREN")
        node = SelectNode(table=tableToken)
        self.symbols.append(node)
        return self.__parse_chain(node)
    
    def parse_get(self):
        self.match("GET")
        self.match("LPAREN")
        table = self.match("STRING").value
        columns = []
        
        # Parse colunas se houver
        if self.lookAhead.type == "COMMA":
            self.match("COMMA")
            while self.lookAhead.type == "STRING":
                columns.append(self.match("STRING").value)
                if self.lookAhead.type == "COMMA":
                    self.match("COMMA")
                else:
                    break
        
        self.match("RPAREN")
        node = SelectNode(table=table, columns=columns if columns else ['*'])
        self.symbols.append(node)
        return self.__parse_chain(node)

    def parse_create_table(self):
        self.match("CREATETABLE")
        self.match("LPAREN")
        table_name = self.match("STRING").value
        self.match("RPAREN")
        
        node = createTableNode(table_name)
        
        # Parse column definitions chained
        while self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "COLUMN":
                column_def = self._parse_column_definition()
                node.add_column(column_def)
            else:
                break
        
        return node
    
    def _parse_column_definition(self):
        self.match("COLUMN")
        self.match("LPAREN")
        
        column_name = self.match("STRING").value
        self.match("COMMA")
        
        column_type = self.match("STRING").value
        column_def = {
            'name': column_name,
            'type': column_type,
            'constraints': []
        }
        
    
        while self.lookAhead.type == "COMMA":
            self.match("COMMA")
            
            if self.lookAhead.type == "STRING":
   
                value = self.match("STRING").value
                if value.isdigit():
                    column_def['size'] = int(value)
                else:
                    column_def['constraints'].append(value)
            elif self.lookAhead.type in ["PRIMARYKEY", "NOTNULL", "UNIQUE", "AUTOINCREMENT"]:
                constraint = self.match(self.lookAhead.type).type.lower()
                column_def['constraints'].append(constraint)
        
        self.match("RPAREN")
        return column_def
    
    def parse_insert(self):
        self.match("INSERT")
        self.match("LPAREN")
        table_name = self.match("STRING").value
        self.match("RPAREN")
        
        node = insertNode(table_name)
        
        # Parse .values() chain
        if self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "VALUES":
                self._parse_values(node)
        
        self.symbols.append(node)
        return node
    
    def _parse_values(self, node):
        self.match("VALUES")
        self.match("LPAREN")
        
        # Parse key-value pairs
        while True:
            column = self.match("STRING").value
            self.match("COMMA")
            value = self.match("STRING").value
            
            node.add_value(column, value)
            
            if self.lookAhead.type == "COMMA":
                self.match("COMMA")
            else:
                break
        
        self.match("RPAREN")

    def __parse_chain(self, node):
        while self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "WHERE":
                self.match("WHERE")
                self.match("LPAREN")
                cond = self._parse_condition()
                self.match("RPAREN")
                node.set_where(cond)
            elif self.lookAhead.type == "LIMIT":
                self.match("LIMIT")
                self.match("LPAREN")
                val = int(self.match("STRING").value)
                self.match("RPAREN")
                node.set_limit(val)
            else:
                raise SyntaxError("Método encadeado não reconhecido")
        return node
    
    def _parse_condition(self):
        left = self.match("STRING").value   # COLUNA
        self.match("COMMA")
        op = self.match("STRING").value     # OPERADOR
        self.match("COMMA")
        right = int(self.match("STRING").value)  # VALOR
        return {"ID": left, "EQUALS": op, "NUMBER": right}


#newparseData = "get('users', 'id', 'name', 'age', where('id', '=', '10'))"

queries = [
    "get('users', 'name').where('age', '>', '18')",
    "insert('users').values('name', 'John Doe', 'age', '21')"
]

parser = NixParser()

print("Testing Queries [PARSER]")
for query in queries:
    result = parser.parse(query)
    print(f"Input: {query}")
    print(f"Result: {result}")
    print(f"Symbols: {parser.symbols}") 
    print(f"Parser state: {vars(parser)}")
