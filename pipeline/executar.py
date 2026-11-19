"""Roda o pipeline inteiro: Bronze → Silver → Gold, com verificações de qualidade.

    python -m pipeline.executar --entrada dados/entrada --lake lake
    python -m pipeline.executar --entrada dados/entrada --lake lake --data-referencia 2026-11-19

Sai com código 1 se alguma verificação de qualidade falhar (o pipeline para e avisa).
Gera relatorio.md com o resumo da execução (aparece no resumo do GitHub Actions).
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

from pipeline import bronze, gold, qualidade, silver
from pipeline.sessao import criar_spark


def _tabela_markdown(df, limite: int = 20) -> str:
    linhas = [r.asDict() for r in df.limit(limite).collect()]
    if not linhas:
        return "_(vazio)_"
    colunas = list(linhas[0])
    md = ["| " + " | ".join(colunas) + " |", "|" + "---|" * len(colunas)]
    for l in linhas:
        md.append("| " + " | ".join("" if l[c] is None else str(l[c]) for c in colunas) + " |")
    return "\n".join(md)


def executar(entrada: str, lake: str, data_referencia: date, spark=None) -> dict:
    spark = spark or criar_spark()
    caminhos = {
        "bronze": f"{lake}/bronze/ordens_servico",
        "silver": f"{lake}/silver/ordens_servico",
        "rejeitadas": f"{lake}/silver/ordens_servico_rejeitadas",
        "gold": f"{lake}/gold",
    }
    inicio = time.perf_counter()
    carregados = bronze.carregar(spark, entrada, caminhos["bronze"])
    total_bronze = spark.read.format("delta").load(caminhos["bronze"]).count()

    total_silver, total_rejeitadas = silver.processar(
        spark, caminhos["bronze"], caminhos["silver"], caminhos["rejeitadas"], data_referencia)
    tabela_silver = spark.read.format("delta").load(caminhos["silver"])
    qualidade.exigir(qualidade.verificar_silver(tabela_silver))

    tabelas_gold = gold.processar(spark, caminhos["silver"], caminhos["gold"])
    qualidade.exigir(qualidade.verificar_gold(
        tabela_silver, tabelas_gold["indicadores_mensais"], tabelas_gold["indicadores_equipamento"]))

    return {
        "carregados": carregados,
        "bronze": total_bronze,
        "silver": total_silver,
        "rejeitadas": total_rejeitadas,
        "segundos": round(time.perf_counter() - inicio, 1),
        "caminhos": caminhos,
        "spark": spark,
        "gold": tabelas_gold,
    }


def relatorio(r: dict) -> str:
    spark = r["spark"]
    motivos = (spark.read.format("delta").load(r["caminhos"]["rejeitadas"])
               .groupBy("motivo").count().orderBy("count", ascending=False))
    return "\n".join([
        "## Pipeline de ordens de serviço · ✅ todas as verificações de qualidade passaram",
        "",
        f"Arquivos novos carregados: {', '.join(r['carregados']) or 'nenhum (já estavam na Bronze)'} · tempo: {r['segundos']} s",
        "",
        "| Camada | Linhas |", "|---|---|",
        f"| Bronze (dado cru) | {r['bronze']} |",
        f"| Silver (limpo, sem duplicatas) | {r['silver']} |",
        f"| Quarentena (rejeitadas) | {r['rejeitadas']} |",
        "",
        "### Por que as linhas foram para a quarentena",
        _tabela_markdown(motivos),
        "",
        "### Gold · indicadores por equipamento (ordenado do que falha com mais frequência)",
        _tabela_markdown(r["gold"]["indicadores_equipamento"]),
        "",
    ])


def main(argumentos: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--entrada", default="dados/entrada")
    p.add_argument("--lake", default="lake")
    p.add_argument("--data-referencia", type=date.fromisoformat, default=date.today(),
                   help="datas depois desta são 'no futuro' (padrão: hoje)")
    p.add_argument("--relatorio", default="relatorio.md")
    p.add_argument("--exportar", help="pasta para salvar as tabelas Gold também em CSV")
    a = p.parse_args(argumentos)
    try:
        resultado = executar(a.entrada, a.lake, a.data_referencia)
    except qualidade.DadosReprovados as erro:
        texto = "## Pipeline de ordens de serviço · ❌ PARADO pela verificação de qualidade\n\n" + "\n".join(
            f"- **[{f.camada}] {f.regra}**: {f.detalhe}" for f in erro.falhas) + "\n"
        Path(a.relatorio).write_text(texto, encoding="utf-8")
        print(texto, file=sys.stderr)
        return 1
    texto = relatorio(resultado)
    Path(a.relatorio).write_text(texto, encoding="utf-8")
    if a.exportar:
        for nome, df in resultado["gold"].items():
            df.coalesce(1).write.mode("overwrite").option("header", True).option("sep", ";").csv(f"{a.exportar}/{nome}")
    print(texto)
    return 0


if __name__ == "__main__":
    sys.exit(main())
