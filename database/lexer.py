from ply import lex
import sys

class NixLexer:
    global lex

    tokens = (
        "ID",
        "STRING",
        "NUMBER",
        "COMMA",
        "DOT",
        "LPAREN",
        "RPAREN",
        "EQUALS",
        "GT",
        "LT", # menor que
        "GTE",# maior ou igual
        "LTE",# menor ou igual
        "NE", # diferente
        "SEMICOLON"
    )

    reservedWords = {
        "getAll": "GETALL",
        "get": "GET",
        "where": "WHERE",
        "join": "JOIN",
        "leftJoin": "LEFTJOIN",
        "rightJoin": "RIGHTJOIN",
        "orderby": "ORDERBY",
        "limit": "LIMIT",

        "insert": "INSERT",
        "update": "UPDATE",
        "delete": "DELETE",
        "into": "INTO",
        "values": "VALUES",
        "set": "SET",
        "from": "FROM",

        "createDatabase": "CREATEDATABASE",
        "createTable": "CREATETABLE",
        "dropDatabase": "DROPDATABASE",
        "dropTable": "DROPTABLE",
        "column": "COLUMN",
        "primaryKey": "PRIMARYKEY",
        "foreignKey": "FOREIGNKEY",
        "references": "REFERENCES",
        "autoIncrement": "AUTOINCREMENT",
     
        "varchar": "VARCHAR",
        "int": "INT",
        "text": "TEXT",
        "datetime": "DATETIME",
        "boolean": "BOOLEAN",
        "float": "FLOAT",
        "decimal": "DECIMAL",
        
        "notNull": "NOTNULL",
        "unique": "UNIQUE",
        "default": "DEFAULT",
        
        "as": "AS",
        "true": "TRUE",
        "false": "FALSE",
        "null": "NULL"
    }

    tokens = tokens + tuple(reservedWords.values())
    t_COMMA =  r'\,'
    t_DOT =    r'\.'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_EQUALS = r'\='
    t_GT =     r'\>'
    t_LT =     r'\<'
    t_GTE =    r'\>='
    t_LTE =    r'\<='
    t_NE =     r'\!='
    t_SEMICOLON = r'\;'

    def t_STRING(self, t):
        r'\"([^\\\n]|(\\.))*?\"|\'([^\\\n]|(\\.))*?\''
        t.value = t.value[1:-1]
        return t
    
    def t_NUMBER(self, t):
        r'\d+(\.\d+)?'

        if '.' in t.value:
            t.value = float(t.value)
        else:
            t.value = int(t.value)
        
        return t

    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        t.type = self.reservedWords.get(t.value, 'ID')
        return t
    
    def t_comment(self, t):
        r'\#.*'
        pass

    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    

    def t_error(self, t):
        print(f'Ilegal character {t.value[0]} in line {t.lexer.lineno}')
        t.lexer.skip(1)
    
    def __init__(self, data):
        self.lexer = lex.lex(module=self)
        self.lexer.input(data)


    t_ignore = ' \t'

    def nextToken(self):
        return self.lexer.token()

    def tokenize(self, data):
        self.lexer.input(data)
        tokens = []
        while True:
            tk = self.lexer.token()
            if not tk:
                break
            tokens.append(tk)
        return tokens


# Testando lexer

if __name__ == "__main__":
    test_queries = [
        "createDatabase('mydb')",
        "createTable('users').column('id', 'int', primaryKey).column('name', 'varchar', '255')",
        "insert('users').values('name', 'João', 'age', '25')",
        "update('users').set('name', 'Maria').where('id', '=', '1')",
        "delete('users').where('id', '=', '1')"
    ]
    
    for query in test_queries:
        print(f"\nTesting: {query}")
        lexer = NixLexer(query)
        tokens = lexer.tokenize(query)
        for token in tokens:
            print(f"  {token.type}: {token.value}")