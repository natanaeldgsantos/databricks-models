# COMMAND ----------
# MAGIC %md
# MAGIC # Manutenção em Lote de Tabelas Delta
# MAGIC 
# MAGIC Execute manutenção automatizada em múltiplas tabelas

# COMMAND ----------

# CONFIGURAÇÃO DE TABELAS PARA MANUTENÇÃO
tables_config = [
    {
        "table_name": "main.sales.orders",
        "table_path": "abfss://datalake@storage.dfs.core.windows.net/sales/orders",
        "partition_filter": "order_date >= current_date() - 30",  # Apenas últimos 30 dias
        "vacuum_retain_hours": 168,
        "force_optimize_full": False
    },
    {
        "table_name": "main.customer.profiles", 
        "table_path": "abfss://datalake@storage.dfs.core.windows.net/customer/profiles",
        "partition_filter": None,  # Toda a tabela
        "vacuum_retain_hours": 336,  # 14 dias para dados de cliente
        "force_optimize_full": True  # Liquid clustering com FULL
    },
    {
        "table_name": "main.analytics.events",
        "table_path": "abfss://datalake@storage.dfs.core.windows.net/analytics/events",
        "partition_filter": "event_date >= current_date() - 7",  # Apenas última semana
        "vacuum_retain_hours": 72,  # 3 dias para dados de eventos
        "force_optimize_full": False
    }
]

# COMMAND ----------

# Executar manutenção em todas as tabelas
print("🚀 INICIANDO MANUTENÇÃO EM LOTE")
print("=" * 60)

batch_results = []
total_tables = len(tables_config)

for i, config in enumerate(tables_config, 1):
    print(f"\n📋 Processando tabela {i}/{total_tables}: {config['table_name']}")
    print("-" * 50)
    
    try:
        result = maintenance.execute_full_maintenance(
            table_name=config["table_name"],
            table_path=config.get("table_path"),
            partition_filter=config.get("partition_filter"),
            vacuum_retain_hours=config.get("vacuum_retain_hours", 168),
            force_optimize_full=config.get("force_optimize_full", False)
        )
        
        batch_results.append(result)
        
        # Status resumido
        successful_steps = sum(1 for success in result["steps"].values() if success)
        total_steps = len(result["steps"])
        print(f"✅ Concluída: {successful_steps}/{total_steps} etapas executadas com sucesso")
        
    except Exception as e:
        print(f"❌ Erro na tabela {config['table_name']}: {str(e)}")
        maintenance.log_operation(config['table_name'], "BATCH_ERROR", f"Falha na manutenção: {str(e)}")

# COMMAND ----------

# Relatório consolidado
print("\n📊 RELATÓRIO CONSOLIDADO")
print("=" * 60)

successful_tables = 0
total_optimize_success = 0
total_reorg_success = 0 
total_vacuum_success = 0

for result in batch_results:
    table_name = result["table"]
    steps = result["steps"]
    
    if all(steps.values()):
        successful_tables += 1
    
    total_optimize_success += 1 if steps.get("optimize") else 0
    total_reorg_success += 1 if steps.get("reorg_purge") else 0
    total_vacuum_success += 1 if steps.get("vacuum") else 0

print(f"📈 Resumo de execução:")
print(f"   • Tabelas processadas: {len(batch_results)}")
print(f"   • Tabelas 100% concluídas: {successful_tables}")
print(f"   • OPTIMIZE bem-sucedidos: {total_optimize_success}")
print(f"   • REORG PURGE bem-sucedidos: {total_reorg_success}")
print(f"   • VACUUM bem-sucedidos: {total_vacuum_success}")

# COMMAND ----------

# Salvar resultados do lote
print("\n💾 SALVANDO RESULTADOS DO LOTE")
print("=" * 50)

if batch_results:
    # Converter todos os resultados para DataFrame
    batch_df = spark.createDataFrame(batch_results)
    
    # Adicionar informações do lote
    batch_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))
    batch_df = batch_df.withColumn("batch_size", lit(len(batch_results)))
    
    # Salvar
    batch_df.write.mode("append").saveAsTable("monitoring.delta_batch_maintenance")
    
    print(f"✅ Resultados salvos com batch_id: {batch_id}")
    print(f"🔍 Para consultar: SELECT * FROM monitoring.delta_batch_maintenance WHERE batch_id = '{batch_id}'")
else:
    print("❌ Nenhum resultado para salvar")

# COMMAND ----------

# Criar dashboard de monitoramento
print("\n📊 MÉTRICAS PARA DASHBOARD")
print("=" * 50)

dashboard_metrics = {
    "execution_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    "total_tables_processed": len(batch_results),
    "successful_maintenance": successful_tables,
    "success_rate": round(successful_tables / len(batch_results) * 100, 1) if batch_results else 0,
    "optimize_success_rate": round(total_optimize_success / len(batch_results) * 100, 1) if batch_results else 0,
    "reorg_success_rate": round(total_reorg_success / len(batch_results) * 100, 1) if batch_results else 0,
    "vacuum_success_rate": round(total_vacuum_success / len(batch_results) * 100, 1) if batch_results else 0
}

print(json.dumps(dashboard_metrics, indent=2))

# Salvar métricas para dashboard
metrics_df = spark.createDataFrame([dashboard_metrics])
metrics_df.write.mode("append").saveAsTable("monitoring.delta_maintenance_metrics")

print("\n✅ Métricas salvas em: monitoring.delta_maintenance_metrics")
