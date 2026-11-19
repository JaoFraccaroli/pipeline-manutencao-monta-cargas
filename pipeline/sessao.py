"""Cria a sessão Spark com Delta Lake (roda local, no GitHub Actions ou no Databricks)."""
from __future__ import annotations


# Datas inválidas (ex.: 31/02) precisam virar nulo, e não erro, para a Silver
# poder barrar a linha e explicar o motivo. O Databricks liga o modo ANSI por padrão.
AJUSTES = {
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.ansi.enabled": "false",
}


def ajustar(spark) -> None:
    for chave, valor in AJUSTES.items():
        spark.conf.set(chave, valor)


def criar_spark(nome: str = "pipeline-os"):
    from pyspark.sql import SparkSession

    ativa = SparkSession.getActiveSession()
    if ativa is not None:  # no Databricks a sessão já existe e já tem Delta
        ajustar(ativa)
        return ativa

    from delta import configure_spark_with_delta_pip

    construtor = (
        SparkSession.builder.appName(nome)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Fuso: o Spark usa o mesmo do sistema (não forçar um fuso diferente do Python,
        # senão "05/03 00:00" volta como "05/03 03:00" numa máquina em UTC)
    )
    spark = configure_spark_with_delta_pip(construtor).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    ajustar(spark)
    return spark
