from database.nyx import NixORM
import sqlite3

def testOrm():
    print("=== 1. NIXORM STRING ONLY ===\n")
    
    db = NixORM()
    db.add_table_schema('users', ['id', 'name', 'email', 'age'])
    db.set_debug(True)
    
    print("TESTANDO STRINGS:")
    
    test_queries = [
        "createDatabase('eCom')",
        "createTable('users').column('id', 'INTEGER', 'primarykey').column('name', 'VARCHAR', '100').column('age', 'INTEGER')",
        "insert('users').values('name', 'John', 'age', '25')",
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
    
    print("=== 2. USING INTERFACE ===")
    
    queries = [
        db.get('users', 'id', 'name'),
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

def testOrmWithDb():
    
    conn = sqlite3.connect('test.db')
    conn.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER
        )
    ''')
    
    conn.execute("INSERT INTO users (name, email, age) VALUES ('João', 'joao@email.com', 25)")
    conn.execute("INSERT INTO users (name, email, age) VALUES ('Maria', 'maria@email.com', 30)")
    conn.execute("INSERT INTO users (name, email, age) VALUES ('Pedro', 'pedro@email.com', 22)")
    conn.commit()
    
    db = NixORM(conn)
    db.add_table_schema('users', ['id', 'name', 'email', 'age'])
    
    print("\n=== EXEMPLO COM BANCO REAL ===\n")
    
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
    testOrm()
    #testOrmWithDb()
