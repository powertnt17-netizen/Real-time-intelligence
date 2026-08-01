# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac0bf30b-2622-43e6-9198-d0905b2b34b5",
# META       "default_lakehouse_name": "Slow_variables",
# META       "default_lakehouse_workspace_id": "e9ab2e73-7dab-4a9a-83ba-4f0432562ce1",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac0bf30b-2622-43e6-9198-d0905b2b34b5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --- 1. CONFIGURAÇÃO DO CHECKPOINT ---
# Guarda o estado do streaming para saber onde ficou na última execução
checkpoint_path = "Files/checkpoints/train_fleet_dev"

# --- 2. LER DA TABELA BRUTA (STREAMING) ---
df_stream = spark.readStream.table("dbo.Raw_data_lakehouse") \
    .filter("Tipo_de_Evento IN ('Temperature', 'Humidity', 'Door Open', 'Lights')")

# --- 3. CONFIGURAÇÃO DA JANELA DE ORDENAÇÃO ---
# Particiona por sensor específico para analisar a linha temporal de cada um isoladamente
window_spec = Window.partitionBy("Comboio", "Carruagem", "ID_Equipamento").orderBy("Datahora_Evento")

# --- 4. FUNÇÃO DE PROCESSAMENTO DE CADA LOTE (MICRO-BATCH) ---
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # A. LIMPEZA E ARREDONDAMENTO DE RUÍDO
    # Se for Temperatura ou Humidade, arredonda para 0 casas decimais. Senão, mantém.
    # Fazemos cast para string no final para manter a consistência com variáveis de texto (ex: 'Open')
    cleaned_df = batch_df.withColumn(
        "Valor",
        F.when(
            F.col("Tipo_de_Evento").isin("Temperature", "Humidity"),
            F.round(F.col("Valor").cast("double"), 0).cast("int").cast("string")
        ).otherwise(F.col("Valor"))
    )
    
    # B. DEDUPLICAÇÃO E CÁLCULO DE DATAS (INÍCIO E FIM DENTRO DO LOTE)
    enriched_df = cleaned_df \
        .withColumn("prev_valor", F.lag("Valor").over(window_spec)) \
        .filter("Valor != prev_valor OR prev_valor IS NULL") \
        .withColumn("Datahora_Fim", F.lead("Datahora_Evento").over(window_spec)) \
        .withColumn("Chave_Ativo", F.concat_ws("-", F.col("Comboio"), F.col("Carruagem"))) \
        .drop("prev_valor")
        
    # C. ESCRITA NA TABELA FINAL (LÓGICA SCD TIPO 2)
    try:
        dt = DeltaTable.forName(spark, "dbo.tabela_variaveis_lentas_filtrada")
        
        # Passo C1: Descobrir o primeiro evento deste lote para cada sensor
        # Isto serve para fechar o registo anterior que ficou em aberto (NULL) na tabela Delta
        updates_df = enriched_df.select(
            F.col("Comboio").alias("s_comboio"),
            F.col("Carruagem").alias("s_carruagem"),
            F.col("ID_Equipamento").alias("s_equipamento"),
            F.col("Datahora_Evento").alias("s_inicio")
        )
        
        min_events_df = updates_df.groupBy("s_comboio", "s_carruagem", "s_equipamento") \
            .agg(F.min("s_inicio").alias("min_inicio"))
            
        # Passo C2: Fechar as linhas antigas na tabela de destino
        dt.alias("target").merge(
            min_events_df.alias("source"),
            """
            target.Comboio = source.s_comboio AND 
            target.Carruagem = source.s_carruagem AND 
            target.ID_Equipamento = source.s_equipamento AND 
            target.Datahora_Fim IS NULL AND 
            target.Datahora_Evento < source.min_inicio
            """
        ).whenMatchedUpdate(set = {"Datahora_Fim": "source.min_inicio"}).execute()
        
        # Passo C3: Inserir as novas linhas tratadas deste lote
        dt.alias("target").merge(
            enriched_df.alias("source"),
            """
            target.Comboio = source.Comboio AND 
            target.Carruagem = source.Carruagem AND 
            target.ID_Equipamento = source.ID_Equipamento AND 
            target.Datahora_Evento = source.Datahora_Evento
            """
        ).whenNotMatchedInsertAll().execute()
        
    except Exception as e:
        # Se a tabela ainda não existir no Lakehouse, cria-a pela primeira vez
        enriched_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("dbo.tabela_variaveis_lentas_filtrada")

# --- 5. INICIAR O MOTOR DE STREAMING ---
query = df_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start()

# Aguarda que o lote atual acabe de processar antes de dar a célula como concluída
query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
