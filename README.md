# ShopBrasil – Pipeline de preços por categoria (Airflow)



## Como subir (Docker Desktop / PowerShell)

```powershell
docker compose up -d --build
```

- Airflow: http://localhost:8081 (usuário `admin` / senha `admin`)
- Postgres analítico: `localhost:5434` (db `ecommerce`, user/senha `analytics`)
- As portas 8081/5434 foram escolhidas para não conflitar com outros ambientes Airflow (8080/5432/5433).
- O pool `ecommerce_pool` (2 slots) e a Connection `postgres_analytics` são criados automaticamente.

Rodar a DAG manualmente: `docker compose exec airflow-scheduler airflow dags trigger ecommerce_pricing_pipeline`

Conferir o banco:

```powershell
docker compose exec postgres-analytics psql -U analytics -d ecommerce -c "SELECT * FROM metricas_categoria_snapshot;"
docker compose exec postgres-analytics psql -U analytics -d ecommerce -c "SELECT * FROM metricas_categoria_historico;"
```

Testes: `docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q`

## Estrutura

```
dags/ecommerce_pricing_pipeline.py   # DAG (TaskFlow API)
dags/common/callbacks.py             # on_failure / on_retry / on_success / SLA (alerta simulado)
dags/common/operators.py             # ValidarProdutosOperator
dags/common/metricas.py              # cálculo das métricas (puro, testável)
dags/common/db.py                    # DDL + upsert/append via PostgresHook
tests/test_dag_integrity.py
```

## Grafo

```
[ingestao]  buscar_produtos → validar_produtos → listar_categorias      (linear)
                                                      │
[analise]   calcular_metricas ×N (expand, pool 2) → consolidar_metricas (fan-out → fan-in)
                                                      │
[persistencia]                         salvar_snapshot ‖ salvar_historico
```

## Checklist dos requisitos

| Requisito | Onde |
|---|---|
| TaskFlow API, XCom via return (dados pequenos) | `@dag`/`@task`; só lista de 20 produtos e métricas trafegam |
| Linear, fan-out, fan-in | `ingestao` → `calcular_metricas.expand` → `consolidar_metricas` |
| 06:00 America/Sao_Paulo, `catchup=False` | `schedule="0 6 * * *"`, `pendulum.datetime(..., tz="America/Sao_Paulo")` |
| Retry + exponential backoff | `buscar_produtos`: `retries=4`, `retry_exponential_backoff=True`, `max_retry_delay` |
| try/except + raise | `buscar_produtos` |
| Callbacks de ciclo de vida | failure/retry/success em `buscar_produtos` |
| Dynamic Task Mapping + pool | `.partial(...).expand(categoria=...)`, `pool="ecommerce_pool"` (2 slots) |
| ≥ 2 TaskGroups | `ingestao`, `analise`, `persistencia` |
| PostgresHook + Connection | `AIRFLOW_CONN_POSTGRES_ANALYTICS` → `postgres_analytics` |
| Idempotência | snapshot: `ON CONFLICT (data_referencia, categoria) DO UPDATE`; histórico: PK `(run_id, categoria)` + `DO NOTHING` |
| Opcional: operador customizado | `ValidarProdutosOperator` |
| Opcional: tabela de histórico | `metricas_categoria_historico` (append por execução) |
| Opcional: SLA / alerta | `sla=1h` em `salvar_snapshot` + `sla_miss_callback` + `on_failure_callback` global |

## Entrega

```powershell
git init; git add .; git commit -m "Pipeline Airflow ShopBrasil"
# crie o repositório no GitHub/GitLab, libere acesso de visualização e envie:
git remote add origin <URL>; git push -u origin main
```
