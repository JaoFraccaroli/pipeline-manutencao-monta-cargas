"""Camada SILVER: dados limpos, padronizados e sem duplicatas ("água tratada").

- aplica as regras de limpeza (equipamento, datas, tipo, horas);
- manda para a QUARENTENA as linhas que não dá para confiar, com o motivo;
- remove duplicatas (fica a versão mais recente de cada OS);
- grava com MERGE: OS nova entra, OS existente é atualizada. Rodar duas vezes dá o mesmo resultado.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.cadastro import EQUIPAMENTOS
from pipeline.regras import converter_data, converter_horas, normalizar_equipamento, normalizar_tipo, tem_hora


def limpar(bronze: DataFrame, data_referencia: date) -> DataFrame:
    """Aplica as regras e calcula, para cada linha, a lista de motivos de rejeição (vazia = ok)."""
    df = bronze.select(
        F.trim("numero_os").alias("numero_os"),
        F.col("equipamento").alias("equipamento_original"),
        normalizar_equipamento(F.col("equipamento")).alias("equipamento"),
        normalizar_tipo(F.col("tipo")).alias("tipo"),
        converter_data(F.col("data_abertura")).alias("data_abertura"),
        converter_data(F.col("data_fechamento")).alias("data_fechamento"),
        tem_hora(F.col("data_abertura")).alias("_abertura_tem_hora"),
        tem_hora(F.col("data_fechamento")).alias("_fechamento_tem_hora"),
        F.col("data_abertura").alias("_abertura_texto"),
        F.col("data_fechamento").alias("_fechamento_texto"),
        converter_horas(F.col("horas_paradas")).alias("horas_paradas"),
        F.col("horas_paradas").alias("_horas_texto"),
        F.trim("descricao").alias("descricao"),  # texto inteiro, inclusive com hífen
        F.trim("tecnico").alias("tecnico"),
        F.initcap(F.trim("status")).alias("status"),
        "arquivo_origem", "carregado_em", "linha_no_arquivo",
    )
    conhecidos = list(EQUIPAMENTOS)
    ambas_com_hora = F.col("_abertura_tem_hora") & F.col("_fechamento_tem_hora")
    # Se uma das datas veio sem hora, compara só o DIA (senão "07/04 01:46" -> "07/04" pareceria fechar antes de abrir)
    fecha_antes = F.when(ambas_com_hora, F.col("data_fechamento") < F.col("data_abertura")).otherwise(
        F.to_date("data_fechamento") < F.to_date("data_abertura"))
    vazio = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")  # noqa: E731
    motivos = [
        F.when(vazio("numero_os"), F.lit("número da OS vazio")),
        F.when(F.col("equipamento").isNull() | ~F.col("equipamento").isin(conhecidos),
               F.concat(F.lit("equipamento desconhecido: "), F.coalesce(F.col("equipamento_original"), F.lit("(vazio)")))),
        F.when(F.col("tipo").isNull(), F.lit("tipo de manutenção desconhecido")),
        F.when(F.col("data_abertura").isNull(),
               F.concat(F.lit("data de abertura inválida: "), F.coalesce(F.col("_abertura_texto"), F.lit("(vazia)")))),
        F.when(~vazio("_fechamento_texto") & F.col("data_fechamento").isNull(),
               F.concat(F.lit("data de fechamento inválida: "), F.col("_fechamento_texto"))),
        F.when(F.col("data_fechamento").isNotNull() & F.col("data_abertura").isNotNull() & fecha_antes,
               F.lit("fechamento antes da abertura")),
        F.when(F.to_date("data_abertura") > F.lit(data_referencia), F.lit("data de abertura no futuro")),
        F.when(~vazio("_horas_texto") & F.col("horas_paradas").isNull(),
               F.concat(F.lit("horas paradas inválidas: "), F.col("_horas_texto"))),
        F.when(F.col("horas_paradas") < 0, F.lit("horas paradas negativas")),
    ]
    return df.withColumn("motivos", F.filter(F.array(*motivos), lambda m: m.isNotNull()))


def separar(limpo: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(aprovadas sem duplicatas, rejeitadas com motivo)."""
    rejeitadas = limpo.where(F.size("motivos") > 0).select(
        "numero_os", "equipamento_original", F.concat_ws("; ", "motivos").alias("motivo"),
        "arquivo_origem", "carregado_em",
    )
    # A versão mais recente de cada OS vence (o lote 2 atualiza o lote 1)
    mais_recente = Window.partitionBy("numero_os").orderBy(
        F.col("carregado_em").desc(), F.col("arquivo_origem").desc(), F.col("linha_no_arquivo").desc())
    aprovadas = (
        limpo.where(F.size("motivos") == 0)
        .withColumn("_ordem", F.row_number().over(mais_recente))
        .where("_ordem = 1")
        .select("numero_os", "equipamento", "tipo", "data_abertura", "data_fechamento", "horas_paradas",
                "descricao", "tecnico", "status", "arquivo_origem", "carregado_em")
    )
    return aprovadas, rejeitadas


def gravar(spark: SparkSession, aprovadas: DataFrame, rejeitadas: DataFrame,
           caminho_silver: str, caminho_rejeitadas: str) -> None:
    from delta.tables import DeltaTable

    if (Path(caminho_silver) / "_delta_log").exists():
        (DeltaTable.forPath(spark, caminho_silver).alias("s")
         .merge(aprovadas.alias("n"), "s.numero_os = n.numero_os")
         .whenMatchedUpdateAll(condition="n.carregado_em >= s.carregado_em")
         .whenNotMatchedInsertAll()
         .execute())
    else:
        aprovadas.write.format("delta").save(caminho_silver)
    # A quarentena é refeita a partir da Bronze inteira: sempre reflete o estado atual
    rejeitadas.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(caminho_rejeitadas)


def processar(spark: SparkSession, caminho_bronze: str, caminho_silver: str, caminho_rejeitadas: str,
              data_referencia: date) -> tuple[int, int]:
    bronze = spark.read.format("delta").load(caminho_bronze)
    aprovadas, rejeitadas = separar(limpar(bronze, data_referencia))
    gravar(spark, aprovadas, rejeitadas, caminho_silver, caminho_rejeitadas)
    return (spark.read.format("delta").load(caminho_silver).count(),
            spark.read.format("delta").load(caminho_rejeitadas).count())
