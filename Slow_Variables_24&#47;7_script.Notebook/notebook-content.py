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

# --- 1. Checkpoint Configuration ---
#Saves the streaming state to know where was the last execution 
checkpoint_path = "Files/checkpoints/train_fleet_dev"

# --- 2. Reading from the raw table  (STREAMING) ---
df_stream = spark.readStream.table("dbo.Raw_data_lakehouse") \
    .filter("Tipo_de_Evento IN ('Temperature', 'Humidity', 'Door Open', 'Lights')")

# --- 3.Order window configuration  ---
# Particiona por sensor específico para analisar a linha temporal de cada um isoladamente
window_spec = Window.partitionBy("Comboio", "Carruagem", "ID_Equipamento").orderBy("Datahora_Evento")

# --- 4.Processing function for each batch (MICRO-BATCH) ---
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # A. noise and round
    # If it is temperature or humidity, rounds the values 
    # Cast is done to string in the final to keep the consistent with text variables
    cleaned_df = batch_df.withColumn(
        "Valor",
        F.when(
            F.col("Tipo_de_Evento").isin("Temperature", "Humidity"),
            F.round(F.col("Valor").cast("double"), 0).cast("int").cast("string")
        ).otherwise(F.col("Valor"))
    )
    
    # B. Deduplications and dates calculations
    enriched_df = cleaned_df \
        .withColumn("prev_valor", F.lag("Valor").over(window_spec)) \
        .filter("Valor != prev_valor OR prev_valor IS NULL") \
        .withColumn("Datahora_Fim", F.lead("Datahora_Evento").over(window_spec)) \
        .withColumn("Chave_Ativo", F.concat_ws("-", F.col("Comboio"), F.col("Carruagem"))) \
        .drop("prev_valor")
        
    # C. Writing in the final table (SCD)
    try:
        dt = DeltaTable.forName(spark, "dbo.tabela_variaveis_lentas_filtrada")
        
        # Step  C1: Find out the first event from this batch to each sensore 
        # This is used to close the previous registar that was opened in the delta table
        updates_df = enriched_df.select(
            F.col("Comboio").alias("s_comboio"),
            F.col("Carruagem").alias("s_carruagem"),
            F.col("ID_Equipamento").alias("s_equipamento"),
            F.col("Datahora_Evento").alias("s_inicio")
        )
        
        min_events_df = updates_df.groupBy("s_comboio", "s_carruagem", "s_equipamento") \
            .agg(F.min("s_inicio").alias("min_inicio"))
            
        # Step  C2: Close the lines in the previous lines in the destination table 
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
        
        # Step C3: Insert new columns treated in this lot
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
        # If the table still doesn´t exists in the lakehouse, it creates it for the first time 
        enriched_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("dbo.tabela_variaveis_lentas_filtrada")

# --- 5. Initiate the streaming engine ---
query = df_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start()

# Waits until that the current lot finish to process before considering cell concluded 
query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
