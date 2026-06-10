# ETL Pipeline: dados de entregas logísticas

Pipeline ETL multi-estágio em Python, do dado bruto à carga particionada em Parquet no Cloudflare R2. O domínio é logística de entregas, familiar da minha passagem pela Loggi, e o foco são padrões que aparecem em pipeline de produção: transformação testada, gate de qualidade de dados e carga incremental idempotente.

## O que o projeto demonstra

- Extração a partir de object storage (S3 compatível) com leitura de CSV e Parquet.
- Transformações de negócio: normalização de status, parsing de datas em UTC, cálculo de distância Haversine e colunas de partição.
- Gate de qualidade de dados: um conjunto de checagens que para o pipeline antes de gravar dado ruim no destino.
- Carga incremental por watermark: cada execução processa apenas os registros novos desde a última e avança o marcador.
- Carga particionada em Parquet no padrão `year=/month=/day=`.
- Cobertura de testes para transformação, qualidade e lógica incremental, com lint e CI no GitHub Actions.

## Arquitetura

```
mock/raw  ->  extract  ->  transform  ->  quality gate  ->  incremental filter  ->  load (Parquet particionado)
                                              |                     |
                                          para se                avanca o
                                          critico falha          watermark
```

## Camada de qualidade

Antes da carga, `DeliveryQualityChecker` roda checagens e produz um relatório. Se alguma checagem crítica falha, `report.raise_for_status()` interrompe a execução. Checagens atuais:

- dataset não vazio
- colunas obrigatórias presentes
- `order_id` único
- sem nulos em colunas críticas
- `status` dentro do domínio permitido
- `distance_km` não negativa
- `lead_time_hours` dentro de limites plausíveis

## Carga incremental

O watermark é o maior `created_at` já processado, guardado como um JSON em `_state/watermark.json` no bucket de saída. Em cada execução o pipeline lê o watermark, filtra apenas os registros mais novos, grava as partições e avança o marcador. Numa segunda execução sem dados novos, nada é regravado.

## Estrutura

```
etl-pipeline/
├── etl/
│   ├── extract.py     # leitura de CSV e Parquet no R2
│   ├── transform.py   # regras de negócio e normalização
│   ├── quality.py     # checagens e gate de qualidade
│   ├── state.py       # watermark e filtro incremental
│   ├── load.py        # escrita particionada em Parquet
│   └── mock_data.py   # geração de dados sintéticos para teste
├── tests/             # testes de transform, quality e state
├── config/            # configuração via variáveis de ambiente
└── main.py            # orquestrador
```

## Como rodar

```bash
pip install -r requirements.txt
cp .env.example .env   # preencha as credenciais do R2
python main.py
```

Variáveis de ambiente esperadas: `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_RAW_BUCKET`, `R2_PROCESSED_BUCKET`.

## Testes e lint

```bash
ruff check .
python -m pytest tests/ -v
```

## Observação

Os dados são sintéticos, gerados com cenários de erro propositais (duplicatas, coordenadas inválidas, status variados) para exercitar as transformações e o gate de qualidade. Volume testado em torno de 12 mil registros.

## Autor

Murillo Sezerino, Engenheiro de Dados
[murillosezerino.com](https://murillosezerino.com) · [LinkedIn](https://linkedin.com/in/murillosezerino)
