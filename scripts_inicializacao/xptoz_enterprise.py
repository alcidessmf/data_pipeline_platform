import psycopg2
from faker import Faker
 
DB_CONFIG = {
    "host": "INSERIR HOST AQUI",
    "user": "INSERIR USER AQUI",
    "password": "INSERIR PASSWORD AQUI",
    "port": "5432",
    "dbname": "INSERIR DBNAME AQUI"
}
 
 
def conectar_banco():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None
 
 
def criar_tabela():
    conn = conectar_banco()
 
    if conn:
        cursor = conn.cursor()
 
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users_info (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                idade INT,
                cidade VARCHAR(100)
            );
        """)
 
        conn.commit()
        cursor.close()
        conn.close()
 
 
def gerar_dados(qtd=1000):
    fake = Faker("pt_BR")
 
    return [
        (
            fake.name(),
            fake.email(),
            fake.random_int(min=18, max=80),
            fake.city()
        )
        for _ in range(qtd)
    ]
 
 
def inserir_dados(dados):
    conn = conectar_banco()
 
    if conn:
        cursor = conn.cursor()
 
        query = """
            INSERT INTO users_info (
                nome,
                email,
                idade,
                cidade
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING;
        """
 
        cursor.executemany(query, dados)
 
        conn.commit()
        cursor.close()
        conn.close()
 
        print(f"{len(dados)} registros processados com sucesso!")
 
 
if __name__ == "__main__":
    criar_tabela()
    dados = gerar_dados(1000)
    inserir_dados(dados)
