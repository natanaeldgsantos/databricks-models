
-- CREATE EXTERNAL TABLE
CREATE TABLE catalog.schema.table_name (

  id          BIGINT NOT NULL COMMENT 'Identificador único do evento',
  nome        STRING NOT NULL COMMENT 'Nome do evento ou entidade relacionada',
  data_evento DATE   NOT NULL COMMENT 'Data em que o evento ocorreu',
  valor       DECIMAL(10,2)   COMMENT 'Valor financeiro associado ao evento'

)
USING DELTA
COMMENT "Tabela personalizada de exemplo"
-- PARTITIONED BY (data_evento)
CLUSTER BY (data_evento)
LOCATION "s://<bucket_name>/my/folder/table_name"
TBLPROPERTIES (

    -- Tags Personalizadas 
    'dataOwner' = '',
    'team' = '', 

    -- Mapeamento de colunas e engines para Leitura/Escrita
    'delta.minReaderVersion' = '2',
    'delta.minWriteVersion' = '5', 
    'delta.ColumnMapping.mode' = 'name', 

    -- Coletar estatísticas sobre colunas chaves
    'delta.dataSkippingStatsColumns' = 'data_evento, valor', -- Cuidado em não usar campos string, especialmente com textos/longos

    -- Otimizações para escrita Delta Files
    'delta.autoOptimize.autoCompact' = 'true',   -- Compactação automática
    'delta.autoOptimize.optimizeWrite' = 'true', -- Escrita otimizada

    -- Retenção e Vacuum Defaults
    'delta.deletedFileRetentionDuration' = '30 days', -- Retenção para time travel
    'delta.logRetentionDuration' = '30 days',         -- Histórico de transações

)

-- Nâo é possível configurar em tempo de criação da tabela, execute o alter table após criação.
ALTER TABLE <catalog.schema.table_name> SET OWNER TO `<nome_owner>`;

-- Verifique o limite efetivo antes de executar
SHOW TBLPROPERTIES sua_tabela ('delta.deletedFileRetentionDuration');

-- Ao alterar "delta.dataSkippingStatsColumns" em uma tabela já existente rode
ANALYZE TABLE <catalog.schema.table_name> COMPUTE DELTA STATISTICS;


