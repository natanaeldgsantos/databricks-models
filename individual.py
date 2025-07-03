# COMMAND ----------
# MAGIC %md
# MAGIC # Manutenção Individual de Tabela Delta
# MAGIC 
# MAGIC Execute manutenção em uma tabela específica

# COMMAND ----------

# CONFIGURAÇÕES DA TABELA
table_name = "main.default.minha_tabela"  # ⬅️ ALTERE AQUI
table_path = "abfss://container@storage.dfs.core.windows.net/path/tabela"  # ⬅️ ALTERE AQUI (opcional)

# Configurações de manutenção
partition_filter = None  # Ex: "date >= '2024-01-01'"
vacuum_retain_hours = 168  # 7 dias
force_optimize_full = False  # True para OPTIMIZE FULL em Liquid Clustering

# COMMAND ----------

# Verificar configurações da tabela
print("🔍 ANÁLISE DA TABELA")
print("=" * 50)

details = maintenance.get_table_details(table_name)
if details:
    print(f"📋 Tabela: {table_name}")
    print(f"💾 Formato: {details.get('format', 'N/A')}")
    print(f"📁 Arquivos: {details.get('numFiles', 0):,}")
    print(f"💽 Tamanho lógico: {humanize.naturalsize(details.get('sizeInBytes', 0), binary=True)}")
    print(f"🗑️ Deletion vectors: {'✅ Habilitados' if maintenance.check_deletion_vectors_enabled(table_name) else '❌ Desabilitados'}")
    print(f"🔗 Liquid clustering: {'✅ Habilitado' if maintenance.check_liquid_clustering(table_name) else '❌ Desabilitado'}")
    
    if details.get('numDeletionVectorFiles', 0) > 0:
        print(f"📊 Arquivos deletion vector: {details.get('numDeletionVectorFiles', 0):,}")
else:
    print("❌ Erro ao obter detalhes da tabela")

# COMMAND ----------

# Executar análise de saúde
print("\n🏥 ANÁLISE DE SAÚDE DA TABELA")
print("=" * 50)

health_report = maintenance.analyze_table_health(table_name, table_path)

if health_report:
    print(f"📊 Métricas:")
    for metric, value in health_report["metrics"].items():
        print(f"   • {metric}: {value}")
    
    print(f"\n💡 Recomendações:")
    if health_report["recommendations"]:
        for rec in health_report["recommendations"]:
            priority_emoji = "🔴" if rec["priority"] == "HIGH" else "🟡"
            print(f"   {priority_emoji} {rec['action']}: {rec['reason']}")
    else:
        print("   ✅ Nenhuma ação necessária")
else:
    print("❌ Erro na análise de saúde")

# COMMAND ----------

# Executar manutenção completa
print("\n🔧 EXECUTANDO MANUTENÇÃO COMPLETA")
print("=" * 50)

result = maintenance.execute_full_maintenance(
    table_name=table_name,
    table_path=table_path,
    partition_filter=partition_filter,
    vacuum_retain_hours=vacuum_retain_hours,
    force_optimize_full=force_optimize_full
)

# Exibir resultado
print(f"\n📋 RESULTADO DA MANUTENÇÃO")
print("=" * 50)
print(f"🏷️ Tabela: {result['table']}")
print(f"⏰ Início: {result['start_time']}")
print(f"⏰ Fim: {result['end_time']}")

print(f"\n✅ Etapas executadas:")
for step, success in result["steps"].items():
    status = "✅" if success else "❌"
    print(f"   {status} {step.upper()}")

# COMMAND ----------

# Exibir log de operações
print("\n📝 LOG DE OPERAÇÕES")
print("=" * 50)

for log_entry in maintenance.maintenance_log[-10:]:  # Últimas 10 operações
    timestamp = log_entry["timestamp"][:19]  # Remover timezone para legibilidade
    operation = log_entry["operation"]
    message = log_entry["message"]
    duration = log_entry.get("duration_seconds")
    
    duration_text = f" ({duration:.2f}s)" if duration else ""
    print(f"[{timestamp}] {operation}: {message}{duration_text}")

# COMMAND ----------

# Salvar relatório de manutenção
print("\n💾 SALVANDO RELATÓRIO")
print("=" * 50)

# Converter resultado para DataFrame e salvar
report_df = spark.createDataFrame([result])
report_df.write.mode("append").saveAsTable("monitoring.delta_maintenance_reports")

print("✅ Relatório salvo em: monitoring.delta_maintenance_reports")
print(f"🔍 Para consultar: SELECT * FROM monitoring.delta_maintenance_reports WHERE table = '{table_name}' ORDER BY start_time DESC")
