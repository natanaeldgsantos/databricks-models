
## 1. Configuração Inicial da Tabela

1. **Propriedades de Escrita Otimizada**
Defina diretamente na tabela para consistência permanente:

```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'  = 'true'
);
```

    - `optimizeWrite` reorganiza dados *shuffle* antes da escrita para evitar *small files*.
    - `autoCompact` compacta automaticamente pequenos arquivos pós-escrita[^1].
2. **Propriedades de Retenção**
Ajuste conforme sua política de *time travel* e custos de armazenamento:

```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration'         = 'interval 30 days',
  'delta.checkpointInterval'           = '100'
);
```

    - `deletedFileRetentionDuration`: tempo mínimo antes de `VACUUM` limpar arquivos antigos.
    - `logRetentionDuration`: controla o histórico disponível para *time travel*.
    - `checkpointInterval`: reduz número de *checkpoints* gerados no log, acelerando listagem[^2].
3. **Verificação de Layout**
    - Tabelas particionadas: escolha colunas com **cardinalidade moderada** e volume ≥ 1 GB/partição[^3].
    - Tabelas com Liquid Clustering: habilite com `CLUSTER BY` para colunas de alta cardinalidade ou padrões de consulta variáveis[^4].

## 2. Fluxo de Manutenção

### 2.1 Compactação e Otimização (`OPTIMIZE`)

- **Tabelas Particionadas**

```sql
-- Compacta apenas partições recentes para economizar DBUs:
OPTIMIZE catalog.schema.table_name
  WHERE partition_col >= current_date() - 30;
```

*Vantagem:* evita reescrever partições estáveis, reduzindo custo de computação[^5].
- **Tabelas com Liquid Clustering**

```sql
-- Compaction incremental respeitando o clustering definido:
OPTIMIZE catalog.schema.table_name;

-- Recluster completo apenas quando necessário (mudança de chave, grandes inserções):
OPTIMIZE catalog.schema.table_name FULL;
```

*Vantagem:* `FULL` reorganiza todos os *Z-Cubes*, equalizando layout sem alterar TBLPROPERTIES[^6][^4].
- **Parâmetro de Tamanho de Arquivo**
Ajuste do alvo de compactação, se necessário:

```python
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "1073741824")  # 1 GB
```


### 2.2 Purga de Dados *Soft-Deleted* (`REORG … APPLY (PURGE)`)

- **Quando usar:** em tabelas habilitadas para *deletion vectors* (`'delta.enableDeletionVectors' = 'true'`) para remover fisicamente linhas marcadas como excluídas sem reescrever toda a partição.
- **Comando**:

```sql
REORG TABLE catalog.schema.table_name
APPLY (PURGE);
```

*Fluxo típico:*

1. `REORG … APPLY (PURGE)` → reescreve apenas arquivos com *soft-deletes*.
2. `VACUUM` → remove fisicamente arquivos antigos e binários de *deletion vector*.


### 2.3 Limpeza de Arquivos (`VACUUM`)

- **Retenção**: normalmente `RETAIN 168 HOURS` (7 dias).
- **Práticas de desempenho**[^2]:
    - Evite **over-partitioning**.
    - Use **auto-scaling** com **compute-optimized workers** (F na Azure, C5 na AWS).
    - Agende `VACUUM` em **job clusters**, não all-purpose.
    - Implemente **DRY RUN** para monitorar progressão e identificar arquivos elegíveis sem deletá-los.

```python
from pyspark.sql import SparkSession

def vacuum_table(table_path, retain_hours=168):
    spark = SparkSession.builder.getOrCreate()
    # Dry run (opcional)
    spark.sql(f"VACUUM delta.`{table_path}` DRY RUN")
    # Execução real
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS")
```


## 3. Orquestração e Cluster

- **Job Clusters Auto-Scaling**
    - **Drivers** robustos (≥ 8 cores) para listagem paralela no `VACUUM`.
    - **Workers** 1–4 auto-escaláveis para acelerar listagem e depois diminuir.
    - **Autotermination** curta (10 min) para evitar ociosidade[^2].
- **Agendamento**
    - `OPTIMIZE` diário/semanal conforme volume de ingestão.
    - `REORG … APPLY (PURGE)` mensal (apenas se usar deletion vectors).
    - `VACUUM` diário (após `OPTIMIZE` e/ou `REORG`) para manter custo de armazenamento sob controle.


## 4. Monitoramento e Alertas

- **Métricas-chave via `DESCRIBE DETAIL`**
    - `sizeInBytes`: tamanho lógico referenciado.
    - `numFiles` vs. espaço bruto (via `spark.read.format("binaryFile")`).
    - `numDeletionVectorFiles` para acionar *REORG* quando > 5% dos arquivos[^2].
- **Relatórios Automatizados**
Salve resultados em tabela de **monitoramento** e crie alertas quando a razão **bruto/lógico** > 1.3 ou quando a quantidade de small files ultrapassar um limiar.


## 5. Resumo das Estratégias

| Etapa | Ação | Notas |
| :-- | :-- | :-- |
| Configuração Inicial | TBLPROPERTIES: autoOptimize, autoCompact, retention | Garante comportamento consistente, independentemente do cluster |
| Compactação (OPTIMIZE) | `WHERE` em partições para layout Hive | Reduz DBUs; `FULL` em Liquid Clustering para reclustering |
| Purga de *Soft-Deletes* | `REORG … APPLY (PURGE)` | Necessário para deletion vectors antes do VACUUM |
| Limpeza (VACUUM) | Retenção de 7 dias; agendado em job clusters auto-scale | Use DRY RUN para monitorar, evite over-partitioning |
| Orquestração e Cluster Design | Job clusters + auto-scaling + compute-optimized | Custos reduzidos, desempenho garantido |
| Monitoramento | `DESCRIBE DETAIL`, razão físico×lógico, small files | Aciona `OPTIMIZE`, `REORG` e `VACUUM` programaticamente |

Esse conjunto de **boas práticas** e **estratégias** garante que suas tabelas Delta — sejam elas particionadas ou com Liquid Clustering — se mantenham **enxutas**, **performáticas** e com **custo operacional** otimizado, ao longo de todo o ciclo de vida dos dados.

6, 7, 8, 11, 13, 14

<div style="text-align: center">⁂</div>

[^1]: https://delta.io/blog/delta-lake-optimize/

[^2]: https://kb.databricks.com/delta/vacuum-best-practices-on-delta-lake

[^3]: https://docs.delta.io/latest/best-practices.html

[^4]: https://docs.delta.io/latest/delta-clustering.html

[^5]: https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-table-maintenance

[^6]: https://delta.io/blog/liquid-clustering/

[^7]: https://www.prophecy.io/blog/delta-lake-performance-optimization-techniques

[^8]: https://thatfabricguy.com/delta-lake-liquid-clustering-partitioning/

[^9]: https://docs.databricks.com/aws/en/delta/best-practices

[^10]: https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices

[^11]: https://docs.databricks.com/aws/en/lakehouse-architecture/reliability/best-practices

[^12]: https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/reliability/best-practices

[^13]: https://docs.delta.io/latest/optimizations-oss.html

[^14]: https://docs.databricks.com/gcp/en/lakehouse-architecture/performance-efficiency/best-practices

[^15]: https://xebia.com/blog/databricks-lakehouse-optimization-a-deep-dive-into-vacuum/

