"""Camada GOLD: indicadores prontos para relatório ("água engarrafada").

1. indicadores_mensais: por equipamento e mês, quantas OS, quantas corretivas, horas paradas e MTTR.
2. indicadores_equipamento: por equipamento, MTBF (tempo médio entre falhas), última falha e a
   SUGESTÃO da próxima manutenção (última falha + MTBF), a mesma pergunta do TCC.

Termos:
- MTTR (tempo médio de reparo): média de horas paradas nas corretivas.
- MTBF (tempo médio entre falhas): média de dias entre uma corretiva e a seguinte do mesmo equipamento.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.cadastro import EQUIPAMENTOS


def indicadores_mensais(silver: DataFrame) -> DataFrame:
    corretiva = F.col("tipo") == "corretiva"
    return (
        silver.withColumn("mes", F.date_format("data_abertura", "yyyy-MM"))
        .groupBy("equipamento", "mes")
        .agg(
            F.count("*").alias("qtd_os"),
            F.sum(corretiva.cast("int")).alias("qtd_corretivas"),
            F.sum((F.col("tipo") == "preventiva").cast("int")).alias("qtd_preventivas"),
            F.sum((F.col("tipo") == "preditiva").cast("int")).alias("qtd_preditivas"),
            F.round(F.sum(F.coalesce("horas_paradas", F.lit(0.0))), 2).alias("horas_paradas"),
            F.round(F.avg(F.when(corretiva, F.col("horas_paradas"))), 2).alias("mttr_horas"),
        )
        .orderBy("equipamento", "mes")
    )


def indicadores_equipamento(spark: SparkSession, silver: DataFrame) -> DataFrame:
    corretivas = silver.where(F.col("tipo") == "corretiva").select("equipamento", "data_abertura")
    janela = Window.partitionBy("equipamento").orderBy("data_abertura")
    intervalos = corretivas.withColumn(
        "dias_desde_anterior",
        (F.col("data_abertura").cast("long") - F.lag("data_abertura").over(janela).cast("long")) / 86400,
    )
    por_eq = intervalos.groupBy("equipamento").agg(
        F.count("*").alias("total_corretivas"),
        F.round(F.avg("dias_desde_anterior"), 1).alias("mtbf_dias"),
        F.max("data_abertura").alias("ultima_falha"),
    )
    cadastro = spark.createDataFrame(
        [(eq, d["setor"], d["capacidade_kg"]) for eq, d in EQUIPAMENTOS.items()],
        "equipamento string, setor string, capacidade_kg int",
    )
    return (
        cadastro.join(por_eq, "equipamento", "left")
        .withColumn("total_corretivas", F.coalesce("total_corretivas", F.lit(0)))
        .withColumn(
            "proxima_manutencao_sugerida",
            F.when(F.col("mtbf_dias").isNotNull(),
                   F.expr("date_add(to_date(ultima_falha), cast(round(mtbf_dias) as int))")),
        )
        .orderBy(F.col("mtbf_dias").asc_nulls_last())
    )


def processar(spark: SparkSession, caminho_silver: str, caminho_gold: str) -> dict[str, DataFrame]:
    silver = spark.read.format("delta").load(caminho_silver)
    tabelas = {
        "indicadores_mensais": indicadores_mensais(silver),
        "indicadores_equipamento": indicadores_equipamento(spark, silver),
    }
    for nome, df in tabelas.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(f"{caminho_gold}/{nome}")
    return {nome: spark.read.format("delta").load(f"{caminho_gold}/{nome}") for nome in tabelas}
