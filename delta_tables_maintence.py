# COMMAND ----------
# MAGIC %md
# MAGIC # Manutenção Automatizada de Tabelas Delta
# MAGIC 
# MAGIC Este notebook implementa um fluxo completo de manutenção para tabelas Delta incluindo:
# MAGIC - Detecção automática de tipo de tabela (particionada vs liquid clustering)
# MAGIC - Suporte a deletion vectors
# MAGIC - Otimização, purga e limpeza automatizadas

# COMMAND ----------

# Instalação de dependências
%pip install humanize --quiet

# COMMAND ----------

import json
import datetime
import humanize
from typing import Dict, List, Tuple, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, count, when
from delta.tables import DeltaTable

# Configurações globais
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

class DeltaTableMaintenance:
    """ Classe para manutenção automatizada de tabelas Delta no Databricks """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.maintenance_log = []
    
    def get_table_details(self, table_name: str) -> Dict:
        """
        Obtém detalhes completos da tabela Delta
        """
        try:
            details = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first().asDict()
            return json.loads(json.dumps(details, default=str))
        except Exception as e:
            self.log_operation(table_name, "ERROR", f"Erro ao obter detalhes: {str(e)}")
            return None
    
    def check_deletion_vectors_enabled(self, table_name: str) -> bool:
        """
        Verifica se deletion vectors estão habilitados na tabela
        """
        try:
            properties = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            for prop in properties:
                if prop['key'] == 'delta.enableDeletionVectors':
                    return prop['value'].lower() == 'true'
            return False
        except:
            return False
    
    def check_liquid_clustering(self, table_name: str) -> bool:
        """
        Verifica se a tabela usa Liquid Clustering
        """
        try:
            properties = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            for prop in properties:
                if prop['key'] == 'delta.clusteringColumns':
                    return True
            return False
        except:
            return False
    
    def get_physical_size(self, table_path: str) -> int:
        """
        Calcula tamanho físico total da tabela no storage
        """
        try:
            df_ls = (
                self.spark.read.format("binaryFile")
                .option("recursiveFileLookup", "true")
                .load(table_path)
            )
            size_bytes = df_ls.select(_sum("length").alias("total")).first()["total"]
            return int(size_bytes or 0)
        except:
            return 0
    
    def log_operation(self, table_name: str, operation: str, message: str, duration: float = None):
        """
        Registra operação no log de manutenção
        """
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "table": table_name,
            "operation": operation,
            "message": message,
            "duration_seconds": duration
        }
        self.maintenance_log.append(log_entry)
        print(f"[{operation}] {table_name}: {message}")
    
    def optimize_table(self, table_name: str, partition_filter: str = None, 
                      force_full: bool = False) -> bool:
        """
        Executa OPTIMIZE na tabela
        """
        import time
        start_time = time.time()
        
        try:
            # Verifica se é Liquid Clustering
            is_liquid = self.check_liquid_clustering(table_name)
            
            if is_liquid and force_full:
                # OPTIMIZE FULL para Liquid Clustering
                query = f"OPTIMIZE {table_name} FULL"
            elif partition_filter:
                # OPTIMIZE com filtro de partição
                query = f"OPTIMIZE {table_name} WHERE {partition_filter}"
            else:
                # OPTIMIZE padrão
                query = f"OPTIMIZE {table_name}"
            
            self.spark.sql(query)
            
            duration = time.time() - start_time
            self.log_operation(table_name, "OPTIMIZE", f"Concluído com sucesso", duration)
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            self.log_operation(table_name, "OPTIMIZE_ERROR", f"Falha: {str(e)}", duration)
            return False
    
    def reorg_purge_table(self, table_name: str, partition_filter: str = None) -> bool:
        """
        Executa REORG TABLE ... APPLY (PURGE) para purgar deletion vectors
        """
        import time
        start_time = time.time()
        
        try:
            if not self.check_deletion_vectors_enabled(table_name):
                self.log_operation(table_name, "REORG_SKIP", "Deletion vectors não habilitados")
                return True
            
            if partition_filter:
                query = f"REORG TABLE {table_name} WHERE {partition_filter} APPLY (PURGE)"
            else:
                query = f"REORG TABLE {table_name} APPLY (PURGE)"
            
            self.spark.sql(query)
            
            duration = time.time() - start_time
            self.log_operation(table_name, "REORG", f"Purga concluída com sucesso", duration)
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            self.log_operation(table_name, "REORG_ERROR", f"Falha: {str(e)}", duration)
            return False
    
    def vacuum_table(self, table_name: str, retain_hours: int = 168, 
                    dry_run: bool = False) -> bool:
        """
        Executa VACUUM na tabela
        """
        import time
        start_time = time.time()
        
        try:
            dry_run_text = "DRY RUN" if dry_run else ""
            query = f"VACUUM {table_name} {dry_run_text} RETAIN {retain_hours} HOURS"
            
            result = self.spark.sql(query)
            
            if dry_run:
                files_to_delete = result.count()
                self.log_operation(table_name, "VACUUM_DRY_RUN", 
                                 f"{files_to_delete} arquivos seriam removidos")
            else:
                duration = time.time() - start_time
                self.log_operation(table_name, "VACUUM", f"Limpeza concluída", duration)
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            self.log_operation(table_name, "VACUUM_ERROR", f"Falha: {str(e)}", duration)
            return False
    
    def analyze_table_health(self, table_name: str, table_path: str = None) -> Dict:
        """
        Analisa saúde da tabela e recomenda ações de manutenção
        """
        details = self.get_table_details(table_name)
        if not details:
            return None
        
        health_report = {
            "table": table_name,
            "analysis_date": datetime.datetime.utcnow().isoformat() + "Z",
            "metrics": {},
            "recommendations": []
        }
        
        # Métricas básicas
        num_files = details.get('numFiles', 0)
        logical_size = details.get('sizeInBytes', 0)
        num_dv_files = details.get('numDeletionVectorFiles', 0)
        
        health_report["metrics"]["num_files"] = num_files
        health_report["metrics"]["logical_size_gb"] = round(logical_size / (1024**3), 2)
        health_report["metrics"]["deletion_vector_files"] = num_dv_files
        
        # Análise de arquivos pequenos
        if num_files > 1000:
            health_report["recommendations"].append({
                "priority": "HIGH",
                "action": "OPTIMIZE",
                "reason": f"Muitos arquivos pequenos detectados: {num_files}"
            })
        
        # Análise de deletion vectors
        if num_dv_files > 0:
            dv_ratio = num_dv_files / num_files if num_files > 0 else 0
            if dv_ratio > 0.05:  # Mais de 5% dos arquivos têm DV
                health_report["recommendations"].append({
                    "priority": "MEDIUM",
                    "action": "REORG_PURGE",
                    "reason": f"Alto número de deletion vectors: {num_dv_files} ({dv_ratio:.1%})"
                })
        
        # Análise de tamanho físico vs lógico
        if table_path:
            physical_size = self.get_physical_size(table_path)
            if physical_size > logical_size * 1.3:  # 30% overhead
                overhead_gb = (physical_size - logical_size) / (1024**3)
                health_report["metrics"]["physical_size_gb"] = round(physical_size / (1024**3), 2)
                health_report["metrics"]["overhead_gb"] = round(overhead_gb, 2)
                health_report["recommendations"].append({
                    "priority": "HIGH",
                    "action": "VACUUM",
                    "reason": f"Overhead de armazenamento detectado: {overhead_gb:.1f} GB"
                })
        
        return health_report
    
    def execute_full_maintenance(self, table_name: str, table_path: str = None,
                               partition_filter: str = None, 
                               vacuum_retain_hours: int = 168,
                               force_optimize_full: bool = False) -> Dict:
        """
        Executa fluxo completo de manutenção
        """
        maintenance_result = {
            "table": table_name,
            "start_time": datetime.datetime.utcnow().isoformat() + "Z",
            "steps": {
                "analyze": False,
                "optimize": False,
                "reorg_purge": False,
                "vacuum": False
            },
            "health_before": None,
            "health_after": None
        }
        
        # 1. Análise inicial
        try:
            health_before = self.analyze_table_health(table_name, table_path)
            maintenance_result["health_before"] = health_before
            maintenance_result["steps"]["analyze"] = True
        except Exception as e:
            self.log_operation(table_name, "ANALYZE_ERROR", f"Falha na análise: {str(e)}")
        
        # 2. OPTIMIZE
        if self.optimize_table(table_name, partition_filter, force_optimize_full):
            maintenance_result["steps"]["optimize"] = True
        
        # 3. REORG PURGE (apenas se deletion vectors habilitados)
        if self.reorg_purge_table(table_name, partition_filter):
            maintenance_result["steps"]["reorg_purge"] = True
        
        # 4. VACUUM
        if self.vacuum_table(table_name, vacuum_retain_hours):
            maintenance_result["steps"]["vacuum"] = True
        
        # 5. Análise final
        try:
            health_after = self.analyze_table_health(table_name, table_path)
            maintenance_result["health_after"] = health_after
        except Exception as e:
            self.log_operation(table_name, "ANALYZE_FINAL_ERROR", f"Falha na análise final: {str(e)}")
        
        maintenance_result["end_time"] = datetime.datetime.utcnow().isoformat() + "Z"
        return maintenance_result

# COMMAND ----------

# Instanciar a classe de manutenção
maintenance = DeltaTableMaintenance(spark)

print("✅ Classe DeltaTableMaintenance carregada com sucesso!")
print("📊 Funcionalidades disponíveis:")
print("   • analyze_table_health() - Analisa saúde da tabela")
print("   • optimize_table() - Executa OPTIMIZE")
print("   • reorg_purge_table() - Executa REORG PURGE")
print("   • vacuum_table() - Executa VACUUM")
print("   • execute_full_maintenance() - Fluxo completo")
