# Data Pipeline Platform (XPTOZ)

Pipeline de dados em camadas que lê uma tabela de origem no PostgreSQL, transforma os dados com **dbt** e orquestra tudo com **Apache Airflow** através do **Cosmos**. Todo o ambiente roda em Docker.

## Sumário

1. [Visão geral](#visão-geral)
2. [Arquitetura e camadas de dados](#arquitetura-e-camadas-de-dados)
3. [Estrutura do projeto](#estrutura-do-projeto)
4. [Pré-requisitos](#pré-requisitos)
5. [Passo a passo](#passo-a-passo)
---

## Visão geral

| Componente | Função |
|---|---|
| **PostgreSQL (`postgres`)** | Banco do projeto. Guarda a origem (`public`) e todas as camadas geradas (`raw`, `staging`, `intermediate`, `marts`).
| **PostgreSQL (`postgres-airflow`)** | Banco de metadados do Airflow (DAGs, execuções, histórico). Separado do banco do projeto. |
| **dbt (`Instalado em um virtualenv isolado dentro da imagem.`)** | Executa as transformações SQL de cada camada. |
| **Airflow 3.3.1 (`airflow-init, airflow-apiserver, airflow-scheduler, airflow-dag-processor`)** | Orquestra a execução, na ordem correta. |
| **Cosmos (`Instalado diretamente no ambiente`)** | Converte os modelos dbt em tasks do Airflow, uma por modelo. |
| **`xptoz_enterprise.py`** | Script que cria e popula a tabela de origem `public.users_info`. |

## Arquitetura e camadas de dados

Todas as camadas ficam no mesmo banco, cada uma em um schema próprio.

### `public` — origem - `public.users_info`

Tabela `users_info`, criada e populada pelo script `scripts_inicializacao/xptoz_enterprise.py`. É a fonte de dados do projeto, e o dbt apenas lê dela (declarada como `source` em `models/source.yml`).

```Colunas: id, nome, email, idade, cidade.```

### `raw` — dado bruto - `raw.users_info` 

Cópia fiel da origem, com os mesmos registros e colunas e o mesmo nome de tabela (`raw.users_info`). Marca a etapa de aterrissagem do dado na nossa estrutura, sem nenhuma transformação.

```Colunas: id, nome, email, idade, cidade.```

```Modelo: models/raw-area/raw_users_info.sql```


### `staging` — normalização e sanitização - `staging.users_info` 

Camada de limpeza e padronização:

- Todos os campos textuais (`nome`, `email`, `cidade`) em **letras maiúsculas**;
- Remoção de espaços nas pontas (`TRIM`);
- Remoção dos **pronomes de tratamento** no início do nome (`Dr.`, `Dra.`, `Sr.`, `Sra.`), sem diferenciar maiúsculas de minúsculas;
- Criação das colunas de controle `created_at` e `updated_at`, preenchidas com o horário da carga (`NOW()`), que registram quando o dado foi populado e atualizado.

```Colunas: id, nome, email, idade, cidade, created_at, updated_at.```

```Modelo: models/staging-area/stg_users_info.sql```

### `intermediate` — regras de negócio - `intermediate.users_info` 

Cria novas colunas derivadas, aplicando regras de negócio:

- `primeiro_nome` e `ultimo_nome`, separados a partir do nome completo (nomes com uma palavra só ficam com `ultimo_nome` nulo);
- `dominio_email`, com o domínio extraído do e-mail (a parte depois do `@`);
- Demais campos preservados para a análise.

```Colunas: id, nome, primeiro_nome, ultimo_nome, email, dominio_email, idade, cidade, created_at, updated_at.```

```Modelo: models/intermediate-area/int_users_info.sql```

### `marts` — consumo analítico - `marts.users_por_dominio`

Tabela final, agregada, pronta para ser consumida por um **dataviz, um relatório ou outro sistema de dados**. Agrupa os registros por domínio de e-mail e traz a quantidade total de registros de cada um.

```Colunas: dominio_email, total_registros, created_at, updated_at.```

```Modelo: models/marts-area/mart_users_por_dominio.sql```

## Estrutura do projeto

```
Dockerfile                                          # Imagem do Airflow + venv do dbt + Cosmos
docker-compose.yml                                  # Sobe os 2 bancos e os serviços do Airflow
requirements.txt                                    # Lib. Cosmos
requirements-dbt.txt                                # Lib. dbt
.envexemplo                                         # Modelo do arquivo .env
.gitignore
dags/dbt_user_info.py                               # DAG "dbt_users_info" (Cosmos)
scripts_inicializacao/xptoz_enterprise.py           # Cria e popula public.users_info
dbt/
    data_pipeline_platform/                         # Projeto dbt
        dbt_project.yml                             # Schemas, materialização e tags por camada
        macros/
            generate_schema_name.sql                # Faz o schema ser exatamente raw/staging/...
        models/
            source.yml                              # Declara public.users_info como origem
            raw-area/
                raw_users_info.sql                  # Script SQL para gerar a tabela em raw
            staging-area/
                stg_users_info.sql                  # Script SQL para gerar a tabela em staging
            intermediate-area/
                int_users_info.sql                  # Script SQL para gerar a tabela em intermediate
            marts-area/
                mart_users_por_dominio.sql          # Script SQL para gerar a tabela em marts
```

## Pré-requisitos

- **Docker** e **Docker Compose v2** (comando `docker compose`).
- **Python 3** na máquina que vai rodar o script de carga, com as bibliotecas `psycopg2-binary` e `faker`.
- Portas livres: **8080** (Airflow) e **5433** (PostgreSQL do projeto).
- Em Windows, use **WSL2** e mantenha o projeto dentro do sistema de arquivos do Linux (por exemplo `~/projetos/...`).

## Passo a passo

### 1. Preparar o ambiente

**1.1. Criar o arquivo `.env`.** Copie o modelo e preencha os valores:

```bash
cp .envexemplo .env
```

Exemplo de `.env` preenchido para ambiente local:

```properties
AIRFLOW_UID=1000
AIRFLOW_DB_PASSWORD=123456
APP_DB_USER=xptoz
APP_DB_PASSWORD=123456
APP_DB_NAME=xptoz_dw
AIRFLOW_JWT_SECRET=a3f91c7e5b2d48a6c0e9f1b7d3a52e84c6f08b19d7e3a4c5
AIRFLOW_API_SECRET_KEY=e04d7b19c35a82f6d1b07e49a5c3f8620b9d41e7a6c5381f
AIRFLOW_FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
```

| Variável | Para que serve |
|---|---|
| `AIRFLOW_UID` | UID do seu usuário no Linux (`id -u`). Controla a permissão nas pastas montadas. |
| `AIRFLOW_DB_PASSWORD` | Senha do banco de metadados do Airflow. |
| `APP_DB_USER`, `APP_DB_PASSWORD`, `APP_DB_NAME` | Credenciais e nome do banco do projeto. |
| `AIRFLOW_JWT_SECRET` | Chave que assina os tokens JWT usados na comunicação interna (scheduler/tasks <-> api-server). Qualquer texto longo e fixo. |
| `AIRFLOW_API_SECRET_KEY` | Chave que protege a comunicação entre o navegador e a API do Airflow (sessões da interface). Use um valor diferente do JWT. |
| `AIRFLOW_FERNET_KEY` | Chave que criptografa dados sensíveis nos metadados do Airflow. **Precisa ser uma chave Fernet válida** (44 caracteres em base64). |

Para gerar valores próprios:

```bash
# JWT secret e API secret key
openssl rand -hex 32 #Ou pelo site: https://jwtsecretkeygenerator.com/

# Fernet key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**1.2. Criar a pasta de logs.** O `.volumes/` não é versionado. Se a pasta de logs não existir, o Docker a cria como `root` e o Airflow não consegue gravar nela:

```bash
mkdir -p .volumes/logs .volumes/database .volumes/airflow_db
```

### 2. Buildar a imagem

```bash
docker compose build
```

O build faz duas coisas:

1. Cria um virtualenv em `/home/airflow/dbt_venv` e instala nele o `dbt-core` e o `dbt-postgres` (`requirements-dbt.txt`). O dbt fica isolado do Airflow para evitar conflito de dependências.
2. Instala o Astronomer Cosmos no ambiente do Airflow (`requirements.txt`), respeitando o arquivo de *constraints* oficial do Airflow 3.3.1.

O build baixa o arquivo de constraints da internet, então precisa de acesso à rede. Alterou o `Dockerfile` ou algum `requirements*.txt`? Rode o build de novo.

### 3. Subir os containers

```bash
docker compose up -d
```

Serviços iniciados, nesta ordem:

| Serviço | Função |
|---|---|
| `postgres-airflow` | Banco de metadados do Airflow |
| `postgres` | Banco do projeto (porta `5433` no host) |
| `airflow-init` | Executa `db migrate` e termina |
| `airflow-apiserver` | Interface web e API (porta `8080`) |
| `airflow-scheduler` | Agenda e dispara as tasks |
| `airflow-dag-processor` | Lê e interpreta os arquivos da pasta `dags/` |


### 4. Popular a tabela de origem

O script `scripts_inicializacao/xptoz_enterprise.py` é responsável por **criar e popular a tabela `public.users_info`**, a origem dos dados do projeto. 

O script roda **na sua máquina** (fora do Docker) e se conecta ao banco pela porta publicada `5433`. Por isso, execute-o **depois** de `docker compose up -d`, com o container `postgres`, ou caso você tenha um banco apartado só mudar as configurações de conexão no script, para apontar pro banco correto. Nesse caso, lembre de modificar o .env para apontar pro seu banco já criado. 

Para rodar o script de população da origem, deve ser instalado dependências: 

```bash
pip install psycopg2-binary faker
```

Confira se a configuração no topo do script bate com o seu `.env`:

```python
DB_CONFIG = {
    "host": "localhost",
    "user": "xptoz",        # = APP_DB_USER
    "password": "123456",   # = APP_DB_PASSWORD
    "port": "5433",         # porta publicada no docker-compose
    "dbname": "xptoz_dw"    # = APP_DB_NAME
}
```

Execute:

```bash
python scripts_inicializacao/xptoz_enterprise.py
```

### 5. Acessar o Airflow

Abra no navegador:

```
http://localhost:8080
```

Utilizei `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, como se trata de um ambiente de testes, então não precisa-ra de uma tela de login, você vai conseguir acessar diretamente.

### 6. Executar a DAG

A DAG se chama **`dbt_users_info`** e vem **pausada** por padrão.

**Pela interface:**

1. Em *Dags*, localize `dbt_users_info`.
2. Ative o botão de liga/desliga (*unpause*).
3. Clique em **Acionar** (*Trigger*).
4. Abra a execução e acompanhe as quatro tasks ficarem verdes, nesta ordem:

```
public_to_raw.raw_users_info_run
        ▼
raw_to_staging.stg_users_info_run
        ▼
staging_to_intermediate.int_users_info_run
        ▼
intermediate_to_marts.mart_users_por_dominio_run
```