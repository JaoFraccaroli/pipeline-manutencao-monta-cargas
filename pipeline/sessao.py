"""Cria a sessão Spark com Delta Lake (roda local, no GitHub Actions ou no Databricks)."""
from __future__ import annotations


def criar_spark(nome: str = "pipeline-os"):
    from pyspark.sql import SparkSession

    ativa = SparkSession.getActiveSession()
    if ativa is not None:  # no Databricks a sessão já existe e já tem Delta
        return ativa

    from delta import configure_spark_with_delta_pip

    construtor = (
        SparkSession.builder.appName(nome)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # datas inválidas viram nulo (em vez de erro) para a Silver poder barrar e explicar
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
    )
    spark = configure_spark_with_delta_pip(construtor).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
