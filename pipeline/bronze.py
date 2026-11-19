"""Camada BRONZE: guarda os arquivos exatamente como chegaram ("água da nascente").

Tudo é lido como texto, sem corrigir nada, e ganha duas colunas de controle:
de qual arquivo veio e quando foi carregado. Um arquivo já carregado não é
carregado de novo (rodar o pipeline duas vezes não duplica nada).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

COLUNAS = ["numero_os", "equipamento", "tipo", "data_abertura", "data_fechamento",
           "horas_paradas", "descricao", "tecnico", "status"]


def ler_csv(spark: SparkSession, arquivo: str) -> DataFrame:
    df = (
        spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8")
        .option("inferSchema", False)  # bronze não interpreta nada: tudo texto
        .csv(arquivo)
    )
    faltando = [c for c in COLUNAS if c not in df.columns]
    if faltando:
        raise ValueError(f"{Path(arquivo).name}: faltam as colunas {faltando}")
    return df.select(*COLUNAS)


def arquivos_ja_carregados(spark: SparkSession, caminho_bronze: str) -> set[str]:
    if not _existe(caminho_bronze):
        return set()
    return {r.arquivo_origem for r in spark.read.format("delta").load(caminho_bronze)
            .select("arquivo_origem").distinct().collect()}


def carregar(spark: SparkSession, pasta_entrada: str, caminho_bronze: str) -> list[str]:
    """Carrega na Bronze os CSVs novos da pasta de entrada. Devolve os nomes carregados."""
    ja = arquivos_ja_carregados(spark, caminho_bronze)
    novos = [a for a in sorted(Path(pasta_entrada).glob("*.csv")) if a.name not in ja]
    for ordem, arquivo in enumerate(novos):
        df = (
            ler_csv(spark, str(arquivo))
            .withColumn("arquivo_origem", F.lit(arquivo.name))
            .withColumn("carregado_em", F.current_timestamp())
            # posição de cada linha no arquivo: desempata linhas repetidas de forma estável
            .withColumn("linha_no_arquivo", F.monotonically_increasing_id())
        )
        df.write.format("delta").mode("append").save(caminho_bronze)
    return [a.name for a in novos]


def _existe(caminho: str) -> bool:
    return (Path(caminho) / "_delta_log").exists()
