"""Pipeline inteiro de ponta a ponta, com os dados fictícios gerados."""
import datetime as dt
import shutil

import pytest

from dados.gerar import gerar

REF = dt.date(2026, 11, 19)


@pytest.fixture
def entrada(tmp_path):
    pasta = tmp_path / "entrada"
    gerar(pasta)
    return pasta


def test_ponta_a_ponta(spark, entrada, tmp_path):
    from pipeline.executar import executar, relatorio
    r = executar(str(entrada), str(tmp_path / "lake"), REF, spark=spark)
    assert r["carregados"] == ["os_lote1.csv", "os_lote2.csv"]
    assert r["bronze"] == 614 + 163
    assert r["silver"] == 740          # 600 OS do lote 1 + 140 novas do lote 2, sem duplicatas
    assert r["rejeitadas"] == 6        # os 6 defeitos plantados no gerador
    motivos = " | ".join(x.motivo for x in spark.read.format("delta").load(r["caminhos"]["rejeitadas"]).collect())
    for esperado in ["número da OS vazio", "equipamento desconhecido: MC-99", "data de abertura inválida: 31/02/2026",
                     "fechamento antes da abertura", "tipo de manutenção desconhecido", "data de abertura no futuro"]:
        assert esperado in motivos
    texto = relatorio(r)
    assert "✅" in texto and "| Silver (limpo, sem duplicatas) | 740 |" in texto


def test_rodar_duas_vezes_nao_duplica(spark, entrada, tmp_path):
    from pipeline.executar import executar
    lake = str(tmp_path / "lake")
    primeira = executar(str(entrada), lake, REF, spark=spark)
    segunda = executar(str(entrada), lake, REF, spark=spark)
    assert segunda["carregados"] == []
    assert (segunda["bronze"], segunda["silver"], segunda["rejeitadas"]) == (
        primeira["bronze"], primeira["silver"], primeira["rejeitadas"])


def test_carga_incremental_fecha_as_os_abertas(spark, entrada, tmp_path):
    from pyspark.sql import functions as F

    from pipeline.executar import executar
    so_lote1, lake = tmp_path / "so_lote1", str(tmp_path / "lake")
    so_lote1.mkdir()
    shutil.copy(entrada / "os_lote1.csv", so_lote1)
    dia1 = executar(str(so_lote1), lake, REF, spark=spark)
    silver = spark.read.format("delta").load(dia1["caminhos"]["silver"])
    abertas = {x.numero_os for x in silver.where(F.col("status") == "Aberta").collect()}
    assert dia1["silver"] == 600 and abertas

    shutil.copy(entrada / "os_lote2.csv", so_lote1)
    dia2 = executar(str(so_lote1), lake, REF, spark=spark)
    assert dia2["carregados"] == ["os_lote2.csv"] and dia2["silver"] == 740
    silver = spark.read.format("delta").load(dia2["caminhos"]["silver"])
    ainda_abertas = {x.numero_os for x in silver.where(F.col("numero_os").isin(list(abertas))
                                                        & (F.col("status") == "Aberta")).collect()}
    assert ainda_abertas == set()


def test_gold_mostra_o_equipamento_que_mais_falha(spark, entrada, tmp_path):
    """O gerador faz o MC-02 falhar mais: ele precisa ter o menor MTBF."""
    from pipeline.executar import executar
    r = executar(str(entrada), str(tmp_path / "lake"), REF, spark=spark)
    primeiro = r["gold"]["indicadores_equipamento"].first()
    assert primeiro.equipamento == "MC-02"


def test_linha_de_comando_para_quando_a_qualidade_falha(spark, entrada, tmp_path, monkeypatch):
    from pipeline import executar as cli
    from pipeline.qualidade import Falha
    monkeypatch.setattr(cli, "criar_spark", lambda: spark)
    monkeypatch.setattr(cli.qualidade, "verificar_gold", lambda *a: [Falha("gold", "teste", "falha simulada")])
    relatorio = tmp_path / "relatorio.md"
    codigo = cli.main(["--entrada", str(entrada), "--lake", str(tmp_path / "lake"),
                       "--data-referencia", "2026-11-19", "--relatorio", str(relatorio)])
    assert codigo == 1 and "PARADO" in relatorio.read_text(encoding="utf-8")


def test_sessao_ja_existente_recebe_os_ajustes_de_data(spark, entrada, tmp_path):
    """No Databricks a sessão já vem pronta, com ANSI ligado: o pipeline precisa
    desligar, senão a data 31/02 quebra tudo em vez de ir para a quarentena."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
    try:
        from pipeline.executar import executar
        r = executar(str(entrada), str(tmp_path / "lake"), REF, spark=spark)
        assert r["rejeitadas"] == 6
        assert spark.conf.get("spark.sql.ansi.enabled") == "false"
    finally:
        from pipeline.sessao import ajustar
        ajustar(spark)
