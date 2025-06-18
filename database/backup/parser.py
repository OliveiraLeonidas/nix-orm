from lexer import NixLexer
import sys

class Node:
    def toDict(self):
        return self.__dict__

class SelectNode(Node):
    def __init__(self, table, columns=None):
        self.type = 'SELECT'
        self.table = table
        self.columns = columns or ['*']
        self.where = None
        self.limit = None
    
    def set_where(self, condition):
        self.where = condition
        return self
    
    def set_limit(self, limit):
        self.limit = limit
        return self
    

class CreateDatabaseNode(Node):
    def __init__(self, database_name):
        self.type = "CREATE DATABASE"
        self.database_name = database_name

class CreateTableNode(Node):
    def __init__(self, table_name):
        self.type = "CREATE TABLE"
        self.table_name = table_name
        self.columns = []

    def add_column(self, column_def):
        self.columns.append(column_def)

class InsertNode(Node):
    def __init__(self, table_name):
        self.type = 'INSERT'
        self.table_name = table_name
        self.values = {}
    
    def add_value(self, column, value):
        self.values[column] = value

class DeleteNode(Node):
    def __init__(self, table_name):
        self.type = 'DELETE'
        self.table_name = table_name
        self.where = None
    
    def set_where(self, condition):
        self.where = condition
        return self

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
            self.error(f'Expected {expected_type}, but found end of input')
        elif self.lookAhead.type == expected_type:
            current = self.lookAhead
            self.lookAhead = self.lexer.nextToken()
            return current
        else:
            self.error(f'Expected {expected_type}, but found {self.lookAhead.type}')
    
    def error(self, message):
        raise SyntaxError(f'[Syntax error] {message}')
    
    def parse_expression(self):
        if self.lookAhead.type == 'GETALL':
            return self.parse_getAll()
        elif self.lookAhead.type == 'GET':
            return self.parse_get()
        elif self.lookAhead.type == 'CREATEDATABASE':
            return self.parse_create_database()
        elif self.lookAhead.type == 'CREATETABLE':
            return self.parse_create_table()
        elif self.lookAhead.type == 'INSERT':
            return self.parse_insert()
        elif self.lookAhead.type == 'DELETE':
            return self.parse_delete()
        else:
            raise SyntaxError(f"Unknown expression: {self.lookAhead.type}")
    
    def parse_create_database(self):
        self.match("CREATEDATABASE")
        self.match("LPAREN")
        database_name = self.match("STRING").value
        self.match("RPAREN")
        
        node = CreateDatabaseNode(database_name)
        self.symbols.append(node)
        return node
    
    def parse_getAll(self):
        self.match("GETALL")
        self.match("LPAREN")
        table_token = self.match("STRING").value
        self.match("RPAREN")
        
        node = SelectNode(table=table_token)
        self.symbols.append(node)
        return self._parse_chain(node)
    
    def parse_get(self):
        self.match("GET")
        self.match("LPAREN")
        table = self.match("STRING").value
        columns = []
        
        # Parse columns if present
        while self.lookAhead and self.lookAhead.type == "COMMA":
            self.match("COMMA")
            if self.lookAhead.type == "STRING":
                columns.append(self.match("STRING").value)
            else:
                break
        
        self.match("RPAREN")
        node = SelectNode(table=table, columns=columns if columns else ['*'])
        self.symbols.append(node)
        return self._parse_chain(node)

    def parse_create_table(self):
        self.match("CREATETABLE")
        self.match("LPAREN")
        table_name = self.match("STRING").value
        self.match("RPAREN")
        
        node = CreateTableNode(table_name)
        
        # Parse column definitions chained
        while self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "COLUMN":
                column_def = self._parse_column_definition()
                node.add_column(column_def)
            else:
                break
        
        self.symbols.append(node)
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
        
        while self.lookAhead and self.lookAhead.type == "COMMA":
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
        
        node = InsertNode(table_name)
        
        if self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "VALUES":
                self._parse_values(node)
        
        self.symbols.append(node)
        return node
    
    def _parse_values(self, node):
        self.match("VALUES")
        self.match("LPAREN")
        
        while True:
            column = self.match("STRING").value
            self.match("COMMA")
            value = self.match("STRING").value
            
            node.add_value(column, value)
            
            if self.lookAhead and self.lookAhead.type == "COMMA":
                self.match("COMMA")
            else:
                break
        
        self.match("RPAREN")
    
    def parse_delete(self):
        self.match("DELETE")
        self.match("LPAREN")
        table_name = self.match("STRING").value
        self.match("RPAREN")
        
        node = DeleteNode(table_name)
        
        if self.lookAhead and self.lookAhead.type == "DOT":
            node = self._parse_chain(node)
        
        self.symbols.append(node)
        return node

    def _parse_chain(self, node):
        while self.lookAhead and self.lookAhead.type == "DOT":
            self.match("DOT")
            if self.lookAhead.type == "WHERE":
                self.match("WHERE")
                self.match("LPAREN")
                condition = self._parse_condition()
                self.match("RPAREN")
                node.set_where(condition)
            elif self.lookAhead.type == "LIMIT":
                self.match("LIMIT")
                self.match("LPAREN")
                limit_val = self.match("STRING").value
                self.match("RPAREN")
                node.set_limit(int(limit_val))
            else:
                break
        return node
    
    def _parse_condition(self):
        column = self.match("STRING").value
        self.match("COMMA")
        operator = self.match("STRING").value
        self.match("COMMA")
        value = self.match("STRING").value
        
        return {
            "column": column,
            "operator": operator, 
            "value": value
        }

queries = [
    "createDatabase('users')",
    "get('users', 'name').where('age', '>', '18')",
    "insert('users').values('name', 'John', 'age', '18')"
]

parser = NixParser()

for query in queries:
    result = parser.parse(query)
    print(result.toDict())