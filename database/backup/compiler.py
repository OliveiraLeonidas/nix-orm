from typing import Dict, Optional, Union
from parser import CreateDatabaseNode, SelectNode, CreateTableNode, InsertNode
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, Float, Boolean, DateTime
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLExecutor:
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.last_sql = None
        self.metadata = MetaData()
        self.tables = {}
        self.engine = create_engine(database_url, echo=False)

         # Permite injetar uma conexão existente
        self.engine = None
        if isinstance(database_url, str):
            self.engine = create_engine(database_url, echo=False)
        # Se não for string, assumimos que é uma conexão já estabelecida

    def execute(self, node, return_sql_only: bool = False):
        """Executa um SelectNode"""
        try:
            if isinstance(node, CreateDatabaseNode):
                return self.__execute_create_database(node)
            
            elif isinstance(node, SelectNode):
                return self.__execute_select(node, return_sql_only)
            elif isinstance(node, CreateTableNode):
                if return_sql_only:
                    return self._generate_create_table_sql(node)
                return self.__execute_create_table(node)
            elif isinstance(node, InsertNode):
                if return_sql_only:
                    return self._generate_insert_sql(node)
                return self._execute_insert(node)
            
            else:
                raise ValueError(f"Tipo de nó não suportado: {type(node)}")
                
        except Exception as e:
            logger.error(f"Erro na execução: {e}")
            raise

    def __execute_create_database(self, node: CreateDatabaseNode) -> Dict[str, Union[bool, str]]:
        """Executa a criação de um banco de dados"""
        database_name = node.database_name
        
        # Validação do nome do banco de dados
        if not database_name:
            raise ValueError("Nome do banco de dados não pode estar vazio")
        
        if not isinstance(database_name, str):
            raise TypeError("Nome do banco de dados deve ser uma string")
        
        # Verifica se o banco já existe (dependendo do dialect SQL)
        try:
            if self.database_url.endswith(database_name):
                return {
                    "success": False,
                    "message": f"Banco de dados '{database_name}' já está em uso"
                }
                
            # Cria uma nova engine temporária para criar o banco
            temp_engine = create_engine(self._get_base_connection_url())
            
            with temp_engine.connect() as conn:
                # Verifica se o banco já existe (syntax varia por SGBD)
                if 'postgresql' in self.database_url:
                    result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{database_name}'"))
                    if result.scalar():
                        return {
                            "success": False,
                            "message": f"Banco de dados '{database_name}' já existe"
                        }
                    
                    # Cria o banco no PostgreSQL
                    conn.execute(text(f"CREATE DATABASE {database_name}"))
                    conn.commit()
                    
                elif 'mysql' in self.database_url:
                    result = conn.execute(text(f"SHOW DATABASES LIKE '{database_name}'"))
                    if result.fetchone():
                        return {
                            "success": False,
                            "message": f"Banco de dados '{database_name}' já existe"
                        }
                    
                    # Cria o banco no MySQL
                    conn.execute(text(f"CREATE DATABASE {database_name}"))
                    conn.commit()
                    
                elif 'sqlite' in self.database_url:
                    # SQLite não suporta CREATE DATABASE, apenas cria novo arquivo
                    return {
                        "success": False,
                        "message": "SQLite não suporta criação de múltiplos bancos de dados"
                    }
                else:
                    raise NotImplementedError(f"SGBD não suportado: {self.database_url}")
            
            # Atualiza a engine para usar o novo banco
            self._update_engine_for_new_database(database_name)
            
            return {
                "success": True,
                "message": f"Banco de dados '{database_name}' criado com sucesso",
                "database_name": database_name
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Erro ao criar banco de dados: {e}")
            return {
                "success": False,
                "message": f"Falha ao criar banco de dados: {str(e)}"
            }

    def _get_base_connection_url(self) -> str:
        """Retorna a URL base de conexão sem o nome do banco específico"""
        if 'postgresql' in self.database_url:
            return self.database_url.rsplit('/', 1)[0]
        elif 'mysql' in self.database_url:
            return self.database_url.rsplit('/', 1)[0]
        return self.database_url

    def _update_engine_for_new_database(self, database_name: str):
        """Atualiza a engine para apontar para o novo banco de dados"""
        if 'postgresql' in self.database_url:
            new_url = f"{self._get_base_connection_url()}/{database_name}"
        elif 'mysql' in self.database_url:
            new_url = f"{self._get_base_connection_url()}/{database_name}"
        else:
            new_url = self.database_url  # SQLite não muda
        
        self.database_url = new_url
        self.engine = create_engine(new_url, echo=False)
        self.metadata = MetaData()  # Reseta metadados
        self.tables = {}  # Limpa cache de tabelas

    def __execute_select(self, node: SelectNode, return_sql_only: bool = False):
        sql = self._generate_select_sql(node)
        self.last_sql = sql
        
        if return_sql_only:
            return sql
        
        if self.database_url:
            return self._execute_sql(sql)
        else:
            print(f"SQL Gerado: {sql}")
            return sql

    def __execute_create_table(self, node: CreateTableNode):
        table_name = node.table_name

        columns = []
        for col in node.columns:
            column = self._convert_column_definition(col)
            columns.append(column)
        
        table = Table(
            table_name,
            self.metadata,
            *columns,
            extend_existing=True
        )

        if self.engine is None:
            raise RuntimeError("Database engine is not initialized.")
        with self.engine.connect() as conn:
            table.create(conn, checkfirst=True)
            conn.commit()

        self.tables[table_name] = table

        return {
            "success": True,
            "message": f"Tabela '{table_name}' criada com sucesso",
            "table_name": table_name,
            "columns": [col.name for col in columns]
        }

    def _execute_insert(self, node: InsertNode):
        """Executa inserção usando SQLAlchemy"""
        table_name = node.table_name
        values = node.values
        
        # Recuperar ou refletir tabela
        if table_name not in self.tables:
            self._reflect_table(table_name)
        
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada")
        
        table = self.tables[table_name]
        
        # Converter valores
        converted_values = self._convert_insert_values(table, values)
        
        # Executar INSERT
        with self.engine.connect() as conn:
            result = conn.execute(table.insert().values(converted_values))
            conn.commit()
            inserted_id = result.lastrowid
        
        return {
            "success": True,
            "message": f"Dados inseridos na tabela '{table_name}'",
            "inserted_id": inserted_id,
            "values": converted_values
        }

    def _generate_create_table_sql(self, node: CreateTableNode):
        """Gera SQL CREATE TABLE para compatibilidade"""
        table_name = node.table_name
        columns_sql = []
        
        for col_def in node.columns:
            col_sql = self._column_definition_to_sql(col_def)
            columns_sql.append(col_sql)
        
        sql = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(columns_sql) + "\n)"
        return sql

    def _generate_insert_sql(self, node: InsertNode) -> str:
        """Gera SQL INSERT para compatibilidade"""
        table_name = node.table_name
        columns = list(node.values.keys())
        values = list(node.values.values())
        
        columns_str = ', '.join(f"`{col}`" for col in columns)
        values_str = ', '.join(f"'{val}'" for val in values)
        
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({values_str})"
        return sql

    def _generate_select_sql(self, node: SelectNode) -> str:
        if node.columns == ['*']:
            columns_str = '*'
        else:
            columns_str = ', '.join(f"`{col}`" for col in node.columns)
        
        sql = f"SELECT {columns_str} FROM `{node.table}`"
        
        # Adicionar WHERE
        if node.where:
            where_clause = self._generate_where_clause(node.where)
            sql += f" WHERE {where_clause}"
        
        # Adicionar LIMIT
        if node.limit:
            sql += f" LIMIT {node.limit}"
        
        return sql

    def _generate_where_clause(self, condition: Dict) -> str:
        """Gera cláusula WHERE com chaves corretas"""
        column = condition.get('column')
        operator = condition.get('operator')
        value = condition.get('value')
        
        if not all([column, operator, value]):
            raise ValueError("Condição WHERE incompleta")
        
        # Formatar valor
        try:
            if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                if '.' in value:
                    float(value)
                else:
                    int(value)
                formatted_value = value
            else:
                formatted_value = f"'{value}'"
        except ValueError:
            formatted_value = f"'{value}'"
        
        return f"`{column}` {operator} {formatted_value}"

    def _execute_sql(self, sql: str):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                results = result.fetchall()
                
                # Converter para dicionários
                column_names = list(result.keys())
                return [dict(zip(column_names, row)) for row in results]
        except SQLAlchemyError as e:
            raise RuntimeError(f"Erro ao executar SQL: {e}")

    def _convert_insert_values(self, table: Table, values):
        """Converte valores de inserção para tipos apropriados"""
        converted = {}
        
        for column_name, value in values.items():
            if column_name not in table.columns:
                raise ValueError(f"Coluna '{column_name}' não existe na tabela")
            
            column = table.columns[column_name]
            converted_value = self._convert_value_to_type(value, column.type)
            converted[column_name] = converted_value
        
        return converted

    def _convert_value_to_type(self, value: Optional[str], column_type) -> Optional[Union[int, float, bool, str]]:
        """Converte valor para o tipo da coluna com tratamento de None"""
        if value is None:
            return None
            
        try:
            if isinstance(column_type, Integer):
                return int(value)
            elif isinstance(column_type, Float):
                return float(value)
            elif isinstance(column_type, Boolean):
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on', 't')
                return bool(value)
            else:
                return str(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"Falha ao converter valor '{value}': {e}")
            return str(value)

    def get_last_sql(self) -> str:
        if self.last_sql is None:
            logger.info('missing last sql command')
            return ""
        return self.last_sql

    def _convert_column_definition(self, col_def: Dict) -> Column:
        """Converte definição de coluna para SQLAlchemy Column"""
        name = col_def['name']
        col_type = col_def['type'].lower()
        constraints = col_def.get('constraints', [])
        size = col_def.get('size')
        
        # Mapear tipos
        type_mapping = {
            'int': Integer,
            'varchar': String(size) if size else String(255),
            'text': Text,
            'float': Float,
            'boolean': Boolean,
            'datetime': DateTime
        }
        
        if col_type not in type_mapping:
            raise ValueError(f"Tipo de coluna não suportado: {col_type}")
        
        sqlalchemy_type = type_mapping[col_type]
        
        # Restrições possíveis no sql
        column_args = {}
        if 'primarykey' in constraints:
            column_args['primary_key'] = True
        if 'notnull' in constraints:
            column_args['nullable'] = False
        if 'unique' in constraints:
            column_args['unique'] = True
        if 'autoincrement' in constraints:
            column_args['autoincrement'] = True

        return Column(name, sqlalchemy_type, **column_args)

    def _column_definition_to_sql(self, col_def: Dict):
        """Converte definição de coluna para SQL"""
        name = col_def['name']
        col_type = col_def['type'].upper()
        constraints = col_def.get('constraints', [])
        size = col_def.get('size')
        
        # Ajustar tipo com tamanho
        if col_type == 'VARCHAR' and size:
            col_type = f"VARCHAR({size})"
        
        sql_parts = [f"`{name}`", col_type]
        
        # Adicionar constraints
        if 'primarykey' in constraints:
            sql_parts.append("PRIMARY KEY")
        if 'notnull' in constraints:
            sql_parts.append("NOT NULL")
        if 'unique' in constraints:
            sql_parts.append("UNIQUE")
        if 'autoincrement' in constraints:
            sql_parts.append("AUTOINCREMENT")
        
        return " ".join(sql_parts)

    def _reflect_table(self, table_name: str):
        """Reflete tabela existente do banco"""
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            self.tables[table_name] = table
        except Exception as e:
            logger.error(f"Erro ao refletir tabela '{table_name}': {e}")

# Função de teste corrigida
def create_test_nodes():
    """Cria nodes de teste com estrutura compatível"""
    
    # Criar tabela de teste
    create_table_node = CreateTableNode(table_name='users')
    create_table_node.columns = [
        {
            'name': 'id',
            'type': 'int',
            'constraints': ['primarykey', 'autoincrement']
        },
        {
            'name': 'name',
            'type': 'varchar',
            'size': 255,
            'constraints': ['notnull']
        },
        {
            'name': 'age',
            'type': 'int',
            'constraints': []
        }
    ]
    
    # Inserir dados de teste
    insert_node = InsertNode(table_name='users')
    insert_node.values = {
        'name': 'João',
        'age': '25'  # String para testar conversão
    }
    
    print('Insert: ', insert_node.toDict(), '\n\n create_table_node: ', create_table_node.toDict())
    builder = SQLExecutor(database_url="")
    sql = builder.execute(insert_node)
    print("SQL: ", sql)

create_test_nodes() 
"""
    create_table_node:  
        {
            'type': 'CREATE TABLE', 
            'table_name': 'users', 
            'columns': [
                {'name': 'id', 'type': 'int',  'constraints': ['primarykey', 'autoincrement']}, 
                {'name': 'name', 'type': 'varchar', 'size': 255, 'constraints': ['notnull']}, 
                {'name': 'age', 'type': 'int', 'constraints': []}
            ]}
"""