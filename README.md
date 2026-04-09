# ETL Pipeline — Cloudflare R2

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458?logo=pandas)
![Cloudflare](https://img.shields.io/badge/Cloudflare_R2-F38020?logo=cloudflare)
![pytest](https://img.shields.io/badge/pytest-7.0-0A9EDC?logo=pytest)

Pipeline ETL end-to-end que processa **12.000+ registros de entregas** de multiplas fontes, aplica transformacoes de negocio e carrega dados particionados no **Cloudflare R2** em formato **Parquet**.

## Arquitetura

```
[0] Mock Data (Faker)
    │  12.000 registros de entregas
    ▼
[1] Upload Raw → R2 (CSV)
    │  raw/deliveries/latest.csv
    ▼
[2] Extract ← R2
    │  R2Extractor.read_csv()
    ▼
[3] Transform (DeliveryTransformer)
    │  ├── Remove duplicatas
    │  ├── Normaliza status (pt→en)
    │  ├── Parse de datas (UTC)
    │  ├── Calcula lead_time_hours
    │  ├── Filtra coordenadas invalidas
    │  ├── Calcula distance_km (Haversine)
    │  └── Adiciona colunas de particao
    ▼
[4] Load → R2 (Parquet particionado)
    deliveries/year=YYYY/month=MM/day=DD/data.parquet
```

## Stack Tecnica

| Tecnologia | Uso |
|---|---|
| Python | Linguagem principal |
| Pandas | Manipulacao de dados |
| NumPy | Calculos numericos (Haversine) |
| boto3 | Client S3-compatible (Cloudflare R2) |
| PyArrow | Leitura/escrita Parquet |
| Faker | Geracao de dados mock |
| pytest | Testes unitarios |

## Transformacoes

O `DeliveryTransformer` utiliza um **builder pattern** encadeavel:

```python
df_clean = DeliveryTransformer.run_all(df_raw)
```

| Etapa | Descricao |
|---|---|
| `remove_duplicates()` | Remove duplicatas por `order_id` |
| `normalize_status()` | Traduz status (entregue→delivered, cancelado→cancelled, etc.) |
| `parse_dates()` | Converte `created_at` e `delivered_at` para datetime UTC |
| `add_lead_time()` | Calcula tempo de entrega em horas |
| `filter_valid_coordinates()` | Remove registros com lat/lng fora do range |
| `add_distance_km()` | Distancia Haversine entre origem e destino |
| `add_partition_columns()` | Adiciona colunas year, month, day para particionamento |

## Estrutura do Projeto

```
├── main.py              # Orquestrador do pipeline
├── etl/
│   ├── extract.py       # R2Extractor — leitura de dados
│   ├── transform.py     # DeliveryTransformer — regras de negocio
│   ├── load.py          # R2Loader — upload CSV/Parquet
│   └── mock_data.py     # Geracao de dados fake
├── config/
│   └── settings.py      # Variaveis de ambiente
├── tests/
│   └── test_transform.py # Testes do transformer
├── requirements.txt
└── .github/workflows/
    └── ci.yml           # CI/CD automatizado
```

## Como Rodar

### Pre-requisitos

- Python 3.11+
- Conta Cloudflare com R2 habilitado

### Setup

```bash
# Clonar o repositorio
git clone https://github.com/murillosezerino/etl-pipeline.git
cd etl-pipeline

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### Configurar Variaveis de Ambiente

Crie um arquivo `.env` na raiz:

```env
R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=sua_access_key
R2_SECRET_ACCESS_KEY=sua_secret_key
R2_RAW_BUCKET=etl-pipeline-raw
R2_PROCESSED_BUCKET=etl-pipeline-processed
LOG_LEVEL=INFO
```

### Executar

```bash
# Rodar pipeline completo
python main.py

# Rodar testes
pytest tests/ -v
```

## Testes

```bash
# Executar todos os testes
pytest tests/ -v --tb=short

# Com cobertura
pytest tests/ -v --cov=etl --cov-report=term-missing
```

## Licenca

MIT
