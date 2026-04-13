# Lab02_NUSP

## Visão Geral do Projeto

Este repositório implementa um pipeline de dados com duas camadas principais:

- **Worker Python**: ingere dados brutos em CSV, cria a camada de staging e gera tabelas dimensionais e de ponte no PostgreSQL.
- **DBT**: consome as tabelas fontes em `public` e materializa modelos em duas camadas principais: `staging` e `gold`.

O projeto foca em dados de filmes, com tabelas de fatos que combinam filmes, gêneros e países de produção.

## Como Reproduzir o Ambiente DBT

### 1. Pré-requisitos

- Python 3.12+ instalado.

### 2. Subir o ambiente Docker

No diretório raiz do projeto:

```powershell
docker-compose up -d --build
```

Isso cria os serviços:

- `postgres_database`: banco PostgreSQL exposto em `localhost:5432`.
- `worker`: componente Python que processa os dados brutos e popula o banco.

### 3. Instalar dependências Python

No diretório raiz, caso queira usar o pacote local:

```powershell
python -m pip install pandas numpy pyarrow sqlalchemy psycopg2-binary tabulate
```

### 4. Instalar dbt

Instale o dbt para PostgreSQL:

```powershell
python -m pip install dbt-core dbt-postgres
```

### 5. Configurar o perfil dbt

Crie ou edite o arquivo `~/.dbt/profiles.yml` com o seguinte conteúdo:

```yaml
dbt_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: postgres
      schema: public
      threads: 1
      keepalives: 1
```

### 6. Executar dbt

```powershell
cd dbt_project
dbt deps
dbt debug
dbt run
dbt test
```

### 7. Gerar e visualizar a documentação dbt

```powershell
dbt docs generate
dbt docs serve
```

## Estrutura do Projeto

- `docker-compose.yml`: define PostgreSQL e o container `worker`.
- `worker/`: código Python de ingestão e processamento das camadas raw, silver e gold.
- `dbt_project/`: projeto dbt com modelos, macros e definições de fontes.
- `dbt_project/models/marts/`: modelos finais de fatos.
- `dbt_project/models/staging/`: definições de fonte e testes de qualidade.

## Fluxo de Dados

1. O serviço `RawLayerProcessor` lê arquivos CSV brutos de `worker/archive`.
2. A camada `SilverLayerProcessor` limpa e transforma os dados em `staging_movies` no PostgreSQL.
3. A camada `GoldLayerProcessor` cria dimensões e tabelas de ponte no banco, preparando os dados para BI.
4. O dbt consome essas tabelas fontes e executa modelos para produzir fatos como:
   - `fact_movies_country`
   - `fact_movies_genre`

## Detalhes do DBT

- O arquivo `dbt_project/dbt_project.yml` define as materializações padrão:
  - `staging` como `view`
  - `marts` como `table`
- Os modelos finais unem filmes, géneros e países para análises de receita, lucro e popularidade.
