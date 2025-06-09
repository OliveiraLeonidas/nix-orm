from database.nyx import NixORM

def exemplo_uso_completo():
    """Demonstra todos os usos possíveis"""
    
    print("=== NIXORM STRING ONLY [PARSER] ===\n")
    
    # Criar ORM
    db = NixORM()
    db.add_table_schema('users', ['id', 'name', 'email', 'age'])
    db.set_debug(True)
    
    print("TESTANDO STRINGS:")
    
    # Testes com strings
    test_queries = [
        "get('users', 'id', 'name', 'age')",
        "getAll('users')",
        "get('users').where('id', '=', '10')",
        "get('users', 'name').where('age', '>', '18').limit('5')"
    ]
    
    for query_str in test_queries:
        try:
            print(f"   Query: {query_str}")
            sql = db.sql(query_str)
            print(f"   SQL: {sql}")
            print()
        except Exception as e:
            print(f"   Erro: {e}\n")
    
    print("2. USING INTERFACE FLUENTE:")
    
    # Interface fluente
    queries = [
        db.get('users', 'id', 'name', 'age'),
        db.getAll('users'),
        db.get('users').where('id', '=', '10'),
        db.get('users', 'name').where('age', '>', '18').limit(5)
    ]
    
    for query in queries:
        try:
            print(f"   Query: {query}")
            print(f"   SQL: {query.sql()}")
            print()
        except Exception as e:
            print(f"   Erro: {e}\n")

def exemplo_com_banco():
    """Exemplo com banco SQLite real"""
    
    # Criar banco
    conn = sqlite3.connect(':memory:')
    conn.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER
        )
    ''')
    
    # Dados de teste
    conn.execute("INSERT INTO users (name, email, age) VALUES ('João', 'joao@email.com', 25)")
    conn.execute("INSERT INTO users (name, email, age) VALUES ('Maria', 'maria@email.com', 30)")
    conn.execute("INSERT INTO users (name, email, age) VALUES ('Pedro', 'pedro@email.com', 22)")
    conn.commit()
    
    # ORM com conexão
    db = NixORM(conn)
    db.add_table_schema('users', ['id', 'name', 'email', 'age'])
    
    print("\n=== EXEMPLO COM BANCO REAL ===\n")
    
    # Executar queries usando strings
    print("1. Todos os usuários:")
    results = db.query("getAll('users')")
    for user in results:
        print(f"   {user}")
    
    print("\n2. Usuários com idade > 24:")
    results = db.query("get('users', 'name', 'age').where('age', '>', '24')")
    for user in results:
        print(f"   {user}")
    
    print("\n3. Primeiro usuário:")
    results = db.query("get('users').limit('1')")
    for user in results:
        print(f"   {user}")
    
    conn.close()

if __name__ == "__main__":
    exemplo_uso_completo()
    #exemplo_com_banco()

    # db = NixORM()
    # result = db.query("get('users', 'id', 'name', 'age')")
    # result = db.query("getAll('users').where('id', '=', '10')")