# COMMAND ----------
# MAGIC %md
# MAGIC # Configuração de Job para Manutenção Automatizada
# MAGIC 
# MAGIC Este notebook deve ser configurado como um Databricks Job agendado

# COMMAND ----------

# Parâmetros do Job (configuráveis via Databricks Jobs UI)
dbutils.widgets.text("catalog_filter", "main", "Catálogo para filtrar tabelas")
dbutils.widgets.text("schema_filter", "*", "Schema para filtrar tabelas (* para todos)")
dbutils.widgets.text("vacuum_retain_hours", "168", "Horas de retenção para VACUUM")
dbutils.widgets.dropdown("maintenance_mode", "conservative", ["conservative", "aggressive"], "Modo de manutenção")

catalog_filter = dbutils.widgets.get("catalog_filter")
schema_filter = dbutils.widgets.get("schema_filter")
vacuum_retain_hours = int(dbutils.widgets.get("vacuum_retain_hours"))
maintenance_mode = dbutils.widgets.get("maintenance_mode")

# COMMAND ----------

# Descoberta automática de tabelas Delta
print("🔍 DESCOBRINDO TABELAS DELTA")
print("=" * 50)

# Listar todos os esquemas do catálogo
if schema_filter == "*":
    schemas_query = f"SHOW SCHEMAS IN {catalog_filter}"
    schemas = [row.schemaName for row in spark.sql(schemas_query).collect()]
else:
    schemas = [schema_filter]

print(f"📋 Esquemas encontrados: {schemas}")

# Descobrir tabelas Delta em cada schema
delta_tables = []
for schema in schemas:
    try:
        tables_query = f"SHOW TABLES IN {catalog_filter}.{schema}"
        tables = spark.sql(tables_query).collect()
        
        for table_row in tables:
            table_name = f"{catalog_filter}.{schema}.{table_row.tableName}"
            
            # Verificar se é tabela Delta
            try:
                details = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
                if details.format.lower() == 'delta':
                    delta_tables.append({
                        "table_name": table_name,
                        "schema": schema,
                        "location": details.location,
                        "num_files": details.numFiles,
                        "size_bytes": details.sizeInBytes
                    })
            except:
                # Não é tabela Delta ou erro de acesso
                continue
                
    except Exception as e:
        print(f"❌ Erro ao processar schema {schema}: {str(e)}")

print(f"✅ {len(delta_tables)} tabelas Delta encontradas")

# COMMAND ----------

# Filtrar tabelas por critérios de manutenção
print("\n🎯 APLICANDO CRITÉRIOS DE MANUTENÇÃO")
print("=" * 50)

maintenance_candidates = []

for table in delta_tables:
    needs_maintenance = False
    priority = "LOW"
    reasons = []
    
    # Critério 1: Muitos arquivos pequenos
    if table["num_files"] > 500:
        needs_maintenance = True
        priority = "HIGH" if table["num_files"] > 2000 else "MEDIUM"
        reasons.append(f"Muitos arquivos: {table['num_files']:,}")
    
    # Critério 2: Tabelas grandes (>1GB) com muitos arquivos
    if table["size_bytes"] > 1e9 and table["num_files"] > 100:
        needs_maintenance = True
        priority = "HIGH"
        reasons.append(f"Tabela grande com fragmentação")
    
    # Critério 3: Modo agressivo - incluir todas as tabelas
    if maintenance_mode == "aggressive":
        needs_maintenance = True
        if not reasons:
            reasons.append("Manutenção preventiva (modo agressivo)")
    
    if needs_maintenance:
        maintenance_candidates.append({
            **table,
            "priority": priority,
            "reasons": reasons
        })

print(f"📊 Tabelas selecionadas para manutenção: {len(maintenance_candidates)}")

# Ordenar por prioridade
priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
maintenance_candidates.sort(key=lambda x: priority_order[x["priority"]])

# COMMAND ----------

# Executar manutenção nas tabelas selecionadas
print("\n🔧 EXECUTANDO MANUTENÇÃO AUTOMÁTICA")
print("=" * 50)

job_results = []
job_start_time = datetime.datetime.utcnow()

for i, candidate in enumerate(maintenance_candidates, 1):
    table_name = candidate["table_name"]
    priority = candidate["priority"]
    
    print(f"\n[{i}/{len(maintenance_candidates)}] 🎯 {priority} - {table_name}")
    print(f"Razões: {', '.join(candidate['reasons'])}")
    
    try:
        # Configurar parâmetros baseados na prioridade
        if priority == "HIGH":
            force_full = True if maintenance.check_liquid_clustering(table_name) else False
            partition_filter = None  # Processar toda a tabela
        else:
            force_full = False
            # Para prioridade baixa/média, processar apenas dados recentes
            partition_filter = "date >= current_date() - 30"  # Ajustar conforme schema
        
        result = maintenance.execute_full_maintenance(
            table_name=table_name,
            table_path=candidate["location"],
            partition_filter=partition_filter,
            vacuum_retain_hours=vacuum_retain_hours,
            force_optimize_full=force_full
        )
        
        # Adicionar metadados do job
        result["job_metadata"] = {
            "priority": priority,
            "reasons": candidate["reasons"],
            "maintenance_mode": maintenance_mode
        }
        
        job_results.append(result)
        
        successful_steps = sum(1 for success in result["steps"].values() if success)
        print(f"✅ {successful_steps}/{len(result['steps'])} etapas concluídas")
        
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        maintenance.log_operation(table_name, "JOB_ERROR", f"Falha no job: {str(e)}")

# COMMAND ----------

# Relatório final do job
print("\n📊 RELATÓRIO FINAL DO JOB")
print("=" * 60)

job_end_time = datetime.datetime.utcnow()
job_duration = (job_end_time - job_start_time).total_seconds()

job_summary = {
    "job_id": datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
    "start_time": job_start_time.isoformat() + "Z",
    "end_time": job_end_time.isoformat() + "Z",
    "duration_seconds": job_duration,
    "catalog_filter": catalog_filter,
    "schema_filter": schema_filter,
    "maintenance_mode": maintenance_mode,
    "vacuum_retain_hours": vacuum_retain_hours,
    "tables_discovered": len(delta_tables),
    "tables_processed": len(job_results),
    "successful_maintenance": sum(1 for r in job_results if all(r["steps"].values())),
    "high_priority_processed": sum(1 for r in job_results if r.get("job_metadata", {}).get("priority") == "HIGH"),
    "medium_priority_processed": sum(1 for r in job_results if r.get("job_metadata", {}).get("priority") == "MEDIUM"),
    "low_priority_processed": sum(1 for r in job_results if r.get("job_metadata", {}).get("priority") == "LOW")
}

print(f"🏷️ Job ID: {job_summary['job_id']}")
print(f"⏱️ Duração: {humanize.naturaldelta(datetime.timedelta(seconds=job_duration))}")
print(f"📋 Tabelas descobertas: {job_summary['tables_discovered']}")
print(f"🔧 Tabelas processadas: {job_summary['tables_processed']}")
print(f"✅ Manutenções bem-sucedidas: {job_summary['successful_maintenance']}")
print(f"🔴 Alta prioridade: {job_summary['high_priority_processed']}")
print(f"🟡 Média prioridade: {job_summary['medium_priority_processed']}")
print(f"🟢 Baixa prioridade: {job_summary['low_priority_processed']}")

# COMMAND ----------

# Salvar resultados do job
print("\n💾 SALVANDO RESULTADOS DO JOB")
print("=" * 50)

# Salvar resumo do job
job_summary_df = spark.createDataFrame([job_summary])
job_summary_df.write.mode("append").saveAsTable("monitoring.delta_job_summaries")

# Salvar resultados detalhados
if job_results:
    job_results_df = spark.createDataFrame(job_results)
    job_results_df = job_results_df.withColumn("job_id", lit(job_summary["job_id"]))
    job_results_df.write.mode("append").saveAsTable("monitoring.delta_job_results")

print(f"✅ Resultados salvos:")
print(f"   • Resumo: monitoring.delta_job_summaries")
print(f"   • Detalhes: monitoring.delta_job_results")
print(f"   • Job ID: {job_summary['job_id']}")

# COMMAND ----------

# Notificação de conclusão (opcional - configurar webhook/email)
print("\n📧 NOTIFICAÇÃO DE CONCLUSÃO")
print("=" * 50)

success_rate = (job_summary["successful_maintenance"] / job_summary["tables_processed"] * 100) if job_summary["tables_processed"] > 0 else 0

notification_message = f"""
🔧 Manutenção Delta Concluída

📊 Resumo:
• Job ID: {job_summary['job_id']}
• Duração: {humanize.naturaldelta(datetime.timedelta(seconds=job_duration))}
• Taxa de sucesso: {success_rate:.1f}%
• Tabelas processadas: {job_summary['tables_processed']}

🎯 Por prioridade:
• 🔴 Alta: {job_summary['high_priority_processed']}
• 🟡 Média: {job_summary['medium_priority_processed']}  
• 🟢 Baixa: {job_summary['low_priority_processed']}

📋 Para mais detalhes:
SELECT * FROM monitoring.delta_job_summaries WHERE job_id = '{job_summary['job_id']}'
"""

print(notification_message)

# Aqui você pode adicionar código para enviar notificações via:
# - Slack webhook
# - Email via SendGrid/AWS SES
# - Teams webhook
# - PagerDuty (em caso de falhas)

print("✅ Job de manutenção concluído com sucesso!")
