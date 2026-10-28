import os
import sys
from pathlib import Path
from dotenv import load_dotenv

import psycopg2
from faker import Faker

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

def obter_variavel(nome, padrao=None):
    valor = os.getenv(nome, padrao)
    if valor is None or valor == "":
        sys.exit(f"Variável {nome} não encontrada. Confira o arquivo {ENV_PATH}")
    return valor


DB_CONFIG = {
    "host": obter_variavel("APP_DB_HOST", "localhost"),
    "port": obter_variavel("APP_DB_PORT", "5433"),
    "user": obter_variavel("APP_DB_USER"),
    "password": obter_variavel("APP_DB_PASSWORD"),
    "dbname": obter_variavel("APP_DB_NAME"),
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
