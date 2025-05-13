from lexer import NixLexer
import ply.yacc as yacc
import sys


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
        return f'<Selected Node: table_name={self.table} where={self.where} limit={self.limit}>'


class NixParser:

    def __init__(self):
        self.lexer = None
        self.lookAhead = None
        self.simbols = {}

    
    def parse(self, data):
        self.lexer = NixLexer(data)
        self.lookAhead = self.lexer.nextToken()
        return self.parse_expression()

    def match(self, expeted_type):
        if self.lookAhead is None:
            self.error(f'Expect {expeted_type}, and end of line found it')
        elif self.lookAhead.type == expeted_type:
            current = self.lookAhead
            self.lookAhead = self.lexer.nextToken()
            return current
        else:
            self.error(f'Expect {expeted_type}, and found type {self.lookAhead.type}')
    
    def error(self, message):
        print('f[Sintax error] {message}')
        sys.exit(1)
    

    def parse_expression(self):
        if self.lookAhead.type == 'GETALL':
            return self.parse_getAll()
        elif self.lookAhead.type == 'GET':
            return self.parse_getAll()
        else:
            raise SyntaxError("Unknow expression")
    
    def parse_getAll(self):
        self.match("GETALL")
        self.match("LPAREN")
        tableToken = self.match("STRING").value
        self.match("RPAREN")
        node = SelectNode(table=tableToken)
        
        return self.__parse_chain(node)

    
    def parse_get(self):
        self.match("GET")
        self.match("LPAREN")
        table = self.match("STRING").value
        columns = []
        while True:
            token = self.match("STRING")
            columns.append(token.value)
            if self.lookAhead.type == "COMMA":
                self.match("COMMA")
            else:
                break
        self.match("RPAREN")
        node = SelectNode(table=table, columns=columns)
        return self.__parse_chain(node)

    #Utilizando encadeamento
    # TODO: teria que gerar uma nova string, uma solução melhor seria dentro dos
    # metodos get e getAll()
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
                val = self.match("NUMBER").value
                self.match("RPAREN")
                node.set_limit(val)
            else:
                raise SyntaxError("Método encadeado não reconhecido")
        return node

    def _parse_condition(self):
        # ID EQUALS NUMBER => id = 1
        left = self.match("ID")
        op = self.match("EQUALS")
        right = self.match("NUMBER")
        return f"{left} {op} {right}"

# Testando Parser

data = "getAll('users').where(id = 1)"
parser = NixParser()

result = parser.parse(data)
print(result.__repr__())