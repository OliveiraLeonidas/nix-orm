from ply.lex import lex
import sys

tokens = (
    "TIPO_DADO",
    "CONSTANTE_CHAR",
    "CONSTANTE_INTEIRA", 
    "CONSTANTE_REAL", 
    "OPERADOR_ARITMETICO", 
    "OPERADOR_ATRIBUICAO", 
    "ABRE_PARENTESES",
    "ABRE_CHAVE",
    "FECHA_PARENTESES",
    "FECHA_CHAVE",
    "FIM_COMANDO",
    "SEPARADOR",
    "MUDA_LINHA",
    "IDENTIFICADOR",
)

t_TIPO_DADO = r"int|float|char|double"
t_OPERADOR_ATRIBUICAO = r"\="
t_CONSTANTE_INTEIRA = r"\d+" 
t_CONSTANTE_REAL = r"[0-9]*.[0-9]*"
t_CONSTANTE_CHAR = r"'[a-zA-Z]'"
t_OPERADOR_ARITMETICO = r"-|\+|\*|\/"
t_ABRE_PARENTESES = r"\("
t_FECHA_PARENTESES = r"\)"
t_ABRE_CHAVE = r"\{"
t_FECHA_CHAVE = r"\}"
t_FIM_COMANDO = r"\;"  
t_SEPARADOR = r"\,"
t_IDENTIFICADOR = r"[a-zA-Z_][a-zA-Z0-9_]*"

def t_MUDA_LINHA(t):
    r"\n"
    t.lexer.lineno += 1

t_ignore = r" "

def t_error(t):
    print(f'Erro: token não reconhecido: {t.value}')
    sys.exit(1)

with open('main.cpp') as arquivo:
    conteudo = arquivo.read()

lexer = lex()


lexer.input(conteudo)

while True:
    t = lexer.token() 
    if not t:
        break
    print(t)