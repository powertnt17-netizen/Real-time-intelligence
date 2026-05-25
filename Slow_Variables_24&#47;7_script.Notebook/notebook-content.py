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

# 1. LER DA TABELA BRUTA
df_stream = spark.readStream.table("dbo.Raw_data_lakehouse") \
    .filter("Tipo_de_Evento IN ('Temperature', 'Humidity', 'Door Open', 'Lights')")

# 2. PARTICIONAR POR COMBOIO, CARRUAGEM E EQUIPAMENTO
window_spec = Window.partitionBy("Comboio", "Carruagem", "ID_Equipamento").orderBy("Datahora_Evento")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    # 3. DEDUPLICAÇÃO DENTRO DO PRÓPRIO LOTE (Micro-batch)
    enriched_df = batch_df.withColumn("prev_valor", F.lag("Valor").over(window_spec)) \
        .filter("Valor != prev_valor OR prev_valor IS NULL") \
        .drop("prev_valor")
        
    # --- A NOVA COLUNA DA CHAVE COMPOSTA ---
    enriched_df = enriched_df.withColumn("Chave_Ativo", F.concat_ws("-", F.col("Comboio"), F.col("Carruagem")))

    # 4. ESCREVER NA TABELA (MERGE PARA EVITAR DUPLICADOS ENTRE LOTES DIFERENTES)
    try:
        # Tenta aceder à tabela de destino
        dt = DeltaTable.forName(spark, "dbo.tabela_variaveis_lentas_filtrada")
        
        # Faz o cruzamento para garantir que o mesmo sensor não envia o mesmo estado no mesmo segundo
        dt.alias("target").merge(
            enriched_df.alias("source"),
            "target.Comboio = source.Comboio AND target.Carruagem = source.Carruagem AND target.ID_Equipamento = source.ID_Equipamento AND target.Valor = source.Valor"
        ).whenNotMatchedInsertAll().execute()
        
    except Exception as e:
        # Se for a primeira execução e a tabela não existir, ele cria-a aqui
        enriched_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("dbo.tabela_variaveis_lentas_filtrada")

# 5. ARRANCAR O MOTOR (Streaming Real-Time 24/7)
query = df_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "Files/checkpoints/train_fleet_v4") \
    .start()

# 6. MANTER O SERVIDOR LIGADO SEM LIMITE
query.awaitTermination()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
