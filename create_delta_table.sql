-- CREATE EXTERNAL TABLE
CREATE TABLE catalog.schema.table_name (

 	id          BIGINT NOT NULL COMMENT 'Identificador único do evento',
 	nome        STRING NOT NULL COMMENT 'Nome do evento ou entidade relacionada',
  data_evento DATE   NOT NULL COMMENT 'Data em que o evento ocorreu',
  valor       DECIMAL(10,2)   COMMENT 'Valor financeiro associado ao evento'

)
USING DELTA
CLUSTER BY ()
COMMENT ""
LOCATION ""
TBLPROPERTIES (

  # Governança e Catalogação
  'dataOwner' = '',
  'team' = '', 

  # Otimizações para escrita Delta Files
  'delta.autoOptimize.autoCompact' = 'true',   -- Compactação automática
  'delta.autoOptimize.optimizeWrite' = 'true', -- Escrita otimizada

  # Retenção e Vacuum Defaults
  'delta.deletedFileRetentionDuration' = '30 days', -- Retenção para time travel
  'delta.logRetentionDuration' = '30 days',         -- Histórico de transações
)

-- 90 dias: 2160 HOURS
-- 60 dias: 1440 HOURS
-- 30 dias: 720  HOURS
-- 15 dias: 360  HOURS
-- 7  dias: 168  HOURS

-- Verifique o limite efetivo antes de executar
SHOW TBLPROPERTIES sua_tabela ('delta.deletedFileRetentionDuration');


VACUUM <catalog.schema.table_name> RETAIN 720 HOURS DRY RUN; -- Simula antes da Execução

f = (
	spark_session.read.table('myTable')

)

# Otimização
df.optimize().executeCompaction()

# Vacuum com retenção alinhada às propriedades

retantion_hours = recuperar dos metadados tbproperties da tabela
df.vacuum(retentionHours=retantion_hours)

