import datetime as dt

import pytest

COLS = "numero_os string, equipamento string, tipo string, data_abertura timestamp, data_fechamento timestamp, " \
       "horas_paradas double, descricao string, tecnico string, status string, arquivo_origem string, carregado_em timestamp"


def os_(n, eq, tipo, dia, horas, mes=3):
    d = dt.datetime(2026, mes, dia, 8)
    return (f"OS-{n:05d}", eq, tipo, d, d, horas, "x", "T", "Fechada", "l.csv", d)


@pytest.fixture
def silver(spark):
    linhas = [
        os_(1, "MC-02", "corretiva", 1, 2.0), os_(2, "MC-02", "corretiva", 11, 4.0),
        os_(3, "MC-02", "corretiva", 31, 6.0), os_(4, "MC-02", "preventiva", 15, 1.0),
        os_(5, "MC-01", "corretiva", 5, 3.0), os_(6, "MC-01", "preditiva", 2, 0.5, mes=4),
    ]
    return spark.createDataFrame(linhas, COLS)


def test_indicadores_mensais(silver):
    from pipeline.gold import indicadores_mensais
    r = {(x.equipamento, x.mes): x.asDict() for x in indicadores_mensais(silver).collect()}
    mc02 = r[("MC-02", "2026-03")]
    assert (mc02["qtd_os"], mc02["qtd_corretivas"], mc02["qtd_preventivas"]) == (4, 3, 1)
    assert mc02["horas_paradas"] == 13.0
    assert mc02["mttr_horas"] == 4.0  # (2 + 4 + 6) / 3: só corretivas
    assert r[("MC-01", "2026-04")]["mttr_horas"] is None  # mês sem corretiva


def test_mtbf_e_proxima_manutencao(spark, silver):
    from pipeline.gold import indicadores_equipamento
    r = {x.equipamento: x.asDict() for x in indicadores_equipamento(spark, silver).collect()}
    # MC-02: corretivas nos dias 1, 11 e 31 -> intervalos de 10 e 20 dias -> MTBF 15
    assert r["MC-02"]["mtbf_dias"] == 15.0
    assert r["MC-02"]["total_corretivas"] == 3
    assert r["MC-02"]["proxima_manutencao_sugerida"] == dt.date(2026, 4, 15)  # 31/03 + 15 dias
    # MC-01: só uma corretiva -> sem MTBF, sem sugestão
    assert r["MC-01"]["mtbf_dias"] is None and r["MC-01"]["proxima_manutencao_sugerida"] is None
    # equipamento sem nenhuma OS aparece com zero
    assert r["MC-06"]["total_corretivas"] == 0
    assert len(r) == 6
