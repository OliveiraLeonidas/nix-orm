from lexer import NixLexer

class NixParser:

    tokens = NixLexer.tokens

# SELECT * FROM users
# GETALL LPAREN STRING column_list RPAREN modifiers    
    def g_query(self, p: str): # select
        p[0] = p[1]


    def g_select(self, p):
        if p[1] == 'getAll':
            p[0] = {'type': 'select', 'table': p[3], 'columns': ['*'], 'modifiers': [5]}
        else:
            p[0] = {'type': 'select', 'table': p[3], 'columns': ['*'], 'modifiers': [5]}

    def g_value(self, p):
        
        if p[1] == 'true': p[0] = True
        elif p[1] == 'false': p[0] = False
        

    