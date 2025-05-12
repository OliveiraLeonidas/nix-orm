from ply import lex, yacc

class NixLexer:
    tokens: str = (
        "ID",
        "STRING",
        "INTEGER",
        "COMMA",
        "DOT",
        "LPAREN",
        "RPAREN",
        "EQUALS",
        "GT", # maior que
        "LT", # menor que
        "GTE",# maior ou igual
        "LTE",# menor ou igual
        "NE" # diferente
    )

    reservedWords: str = {
        "getAll": "GETALL",
        "get": "GET",
        "where": "WHERE",
        "join": "JOIN",
        "leftJoin": "LEFTJOIN",
        "rightJoin": "RIGHTJOIN",
        "order": "ORDER BY",
        "limit": "LIMIT",
        "insert": "INSERT",
        "update": "UPDATE",
        "delete": "DELETE",
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
    
    def __init__(self):
        self.lexer = lex.lexer(module=self)


    t_ignore = ' \t'

    def tokenize(self, data):
        self.lexer.input(data)
        tokens = []
        while True:
            tk = self.lexer.token()
            if not tk:
                break
            tokens.append(tk)
        return tokens
