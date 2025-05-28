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
        return f'<Selected Node: table_name={self.table} or columns name {self.columns} where={self.where} limit={self.limit}>'

class NixParser:

    def __init__(self):
        self.lexer = None
        self.lookAhead = None
        self.simbols = []

    
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
        print(f'[Sintax error] {message}')
        sys.exit(1)
    

    def parse_expression(self):
        if self.lookAhead.type == 'GETALL':
            return self.parse_getAll()
        elif self.lookAhead.type == 'GET':
            return self.get()
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
    # TODO: mover a logica de where que está no método get() para o parse_chain e verificar se não houve quebra de lógica
    # em outras partes do código
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
        # string  ,   string  , string
        # 'COLUMN', 'OPERATOR', ' VALUE'
        
        left = self.match("STRING").value # COLUNA
        self.match("COMMA")
        op = self.match("STRING").value # OPERADOR
        self.match("COMMA")
        right = self.match("STRING").value # VALOR
        return {"ID": left, "EQUALS": op, "NUMBER": right}
    

    def getAll(self):
        self.match("GETALL")
        self.match("LPAREN")
        table = self.match("STRING").value
        self.match("RPAREN")
        node = SelectNode(table=table, columns=['*'])
        print(f'GetAll')
        return node
    
    """ GET WITH WHERE CLAUSE => TOKENIZE EXPRESSION: """
    def get(self):
        # get('users', where("id = 10"))
        # GET LPAREN STRING COMMA STRING COMMA STRING WHERE LPAREN PARSE_CONDITION RPAREN RPAREN
        print('entrou')
        self.match("GET")
        self.match("LPAREN")
        table = self.match("STRING").value # used as string (table name)
        columns = []

        if self.lookAhead.type == "COMMA":
            self.match("COMMA")

            if self.lookAhead.type == "STRING":
                columns.append(self.match("STRING").value)

                while self.lookAhead.type == "COMMA":
                    self.match("COMMA")

                    if self.lookAhead.type == "WHERE":
                        break

                    if self.lookAhead.type == "STRING":
                        columns.append(self.match("STRING").value)
                    
                    else: 
                        return f'Expected STRING or WHERE token, but got {self.lookAhead.type}'
        
        # node = SelectNode(table=table, columns= columns if columns else ['*'])

        if  self.lookAhead.type == "WHERE":
                self.match("WHERE")
                self.match("LPAREN")
                condition = self._parse_condition()
                self.match("RPAREN")
                node = SelectNode(table=table, columns = columns if columns else ['*'])
                print(condition)
                node.set_where(condition=condition)
                self.match("RPAREN")
                self.simbols.append(node)
                return node
                    
        
        self.match("RPAREN")
        return node
        
        

# Testando Parser

data = "getAll('users').where(id = 1)"
# where('id', '=', '10')
newparseData = "get('users', 'id', 'name', 'age', where('id', '=', '10'))"
parser = NixParser()

result = parser.parse(newparseData)
print(parser.simbols)

def get(table, where=None):
    if where == None:
        return f'get{table}'
    
    return f'get({table}, {where})'
        