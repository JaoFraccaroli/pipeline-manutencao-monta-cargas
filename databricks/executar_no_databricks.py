# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline de ordens de serviço no Databricks (Free Edition)
# MAGIC
# MAGIC Roda **o mesmo código** do repositório (`pipeline/`) dentro do Databricks.
# MAGIC
# MAGIC 1. No Databricks, **Workspace → Create → Git folder** e cole a URL deste repositório.
# MAGIC 2. Abra este notebook dentro da pasta clonada e clique em **Run all**.
# MAGIC 3. Para agendar: **Schedule** (canto superior direito) → por exemplo, todo dia às 6h. Vira um *Job*.
# MAGIC
# MAGIC Os dados ficam num **Volume** do Unity Catalog (`/Volumes/workspace/default/ordens_servico`).

# COMMAND ----------

import os
import shutil
import sys
from datetime import date

RAIZ = os.path.abspath(os.path.join(os.getcwd(), ".."))  # pasta do repositório
sys.path.insert(0, RAIZ)

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.ordens_servico")  # noqa: F821 (spark existe no Databricks)
VOLUME = "/Volumes/workspace/default/ordens_servico"
ENTRADA, LAKE = f"{VOLUME}/entrada", f"{VOLUME}/lake"
os.makedirs(ENTRADA, exist_ok=True)

# Simula a chegada dos arquivos: copia os CSVs fictícios do repositório para o Volume
for arquivo in os.listdir(f"{RAIZ}/dados/entrada"):
    if not os.path.exists(f"{ENTRADA}/{arquivo}"):
        shutil.copy(f"{RAIZ}/dados/entrada/{arquivo}", ENTRADA)
print(os.listdir(ENTRADA))

# COMMAND ----------

from pipeline.executar import executar, relatorio  # noqa: E402

resultado = executar(ENTRADA, LAKE, date.today(), spark=spark)  # noqa: F821
print(relatorio(resultado))

# COMMAND ----------

# MAGIC %md ## Gold: equipamentos ordenados do que falha com mais frequência

# COMMAND ----------

display(resultado["gold"]["indicadores_equipamento"])  # noqa: F821

# COMMAND ----------

display(spark.read.format("delta").load(f"{LAKE}/silver/ordens_servico_rejeitadas"))  # noqa: F821
