"""Verificações de qualidade de dados. Se alguma falhar, o pipeline PARA e avisa.

É o "QA de dados": em vez de testar telas, testa se o dado que chegou em cada
camada faz sentido. Cada verificação devolve uma Falha com a explicação.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.cadastro import EQUIPAMENTOS


@dataclass(frozen=True)
class Falha:
    camada: str
    regra: str
    detalhe: str


class DadosReprovados(Exception):
    def __init__(self, falhas: list[Falha]):
        self.falhas = falhas
        super().__init__("; ".join(f"[{f.camada}] {f.regra}: {f.detalhe}" for f in falhas))


def verificar_silver(silver: DataFrame) -> list[Falha]:
    falhas: list[Falha] = []
    total = silver.count()
    if total == 0:
        return [Falha("silver", "não vazia", "a Silver ficou sem nenhuma OS")]
    distintas = silver.select("numero_os").distinct().count()
    if distintas != total:
        falhas.append(Falha("silver", "OS única", f"{total - distintas} OS repetidas"))
    checagens = {
        "número da OS preenchido": F.col("numero_os").isNull() | (F.col("numero_os") == ""),
        "equipamento do cadastro": ~F.col("equipamento").isin(list(EQUIPAMENTOS)),
        "tipo conhecido": ~F.col("tipo").isin("corretiva", "preventiva", "preditiva"),
        "data de abertura preenchida": F.col("data_abertura").isNull(),
        "fechamento não antes da abertura": F.to_date("data_fechamento") < F.to_date("data_abertura"),
        "horas paradas entre 0 e 72": (F.col("horas_paradas") < 0) | (F.col("horas_paradas") > 72),
    }
    for regra, condicao_ruim in checagens.items():
        ruins = silver.where(condicao_ruim).count()
        if ruins:
            falhas.append(Falha("silver", regra, f"{ruins} linha(s) violam a regra"))
    return falhas


def verificar_gold(silver: DataFrame, mensais: DataFrame, por_equipamento: DataFrame) -> list[Falha]:
    falhas: list[Falha] = []
    total_silver = silver.count()
    total_gold = mensais.agg(F.sum("qtd_os")).first()[0] or 0
    if total_gold != total_silver:
        falhas.append(Falha("gold", "total bate com a Silver", f"Gold soma {total_gold} OS, Silver tem {total_silver}"))
    corretivas_silver = silver.where(F.col("tipo") == "corretiva").count()
    corretivas_gold = por_equipamento.agg(F.sum("total_corretivas")).first()[0] or 0
    if corretivas_gold != corretivas_silver:
        falhas.append(Falha("gold", "corretivas batem com a Silver",
                            f"Gold soma {corretivas_gold}, Silver tem {corretivas_silver}"))
    if por_equipamento.count() != len(EQUIPAMENTOS):
        falhas.append(Falha("gold", "um registro por equipamento", f"{por_equipamento.count()} linhas, esperado {len(EQUIPAMENTOS)}"))
    negativos = por_equipamento.where(F.col("mtbf_dias") < 0).count()
    if negativos:
        falhas.append(Falha("gold", "MTBF não negativo", f"{negativos} equipamento(s) com MTBF negativo"))
    return falhas


def exigir(falhas: list[Falha]) -> None:
    if falhas:
        raise DadosReprovados(falhas)
