import datetime as dt

import pytest

from tests.test_gold import COLS, os_


def test_silver_boa_nao_tem_falhas(spark):
    from pipeline.qualidade import verificar_silver
    df = spark.createDataFrame([os_(1, "MC-01", "corretiva", 1, 2.0), os_(2, "MC-02", "preventiva", 2, 1.0)], COLS)
    assert verificar_silver(df) == []


def test_silver_ruim_e_reprovada_com_explicacao(spark):
    from pipeline.qualidade import DadosReprovados, exigir, verificar_silver
    ruins = [os_(1, "MC-01", "corretiva", 1, 2.0), os_(1, "MC-01", "corretiva", 1, 2.0),  # repetida
             os_(2, "MC-77", "corretiva", 2, 2.0), os_(3, "MC-01", "corretiva", 3, 100.0)]
    falhas = verificar_silver(spark.createDataFrame(ruins, COLS))
    regras = {f.regra for f in falhas}
    assert {"OS única", "equipamento do cadastro", "horas paradas entre 0 e 72"} <= regras
    with pytest.raises(DadosReprovados, match="OS única"):
        exigir(falhas)


def test_silver_vazia_e_reprovada(spark):
    from pipeline.qualidade import verificar_silver
    assert verificar_silver(spark.createDataFrame([], COLS))[0].regra == "não vazia"


def test_gold_que_nao_bate_com_a_silver_e_reprovada(spark):
    from pipeline.gold import indicadores_equipamento, indicadores_mensais
    from pipeline.qualidade import verificar_gold
    silver = spark.createDataFrame([os_(1, "MC-01", "corretiva", 1, 2.0), os_(2, "MC-01", "corretiva", 9, 2.0)], COLS)
    mensais = indicadores_mensais(silver).limit(0)  # Gold "perdeu" linhas
    falhas = verificar_gold(silver, mensais, indicadores_equipamento(spark, silver))
    assert [f.regra for f in falhas] == ["total bate com a Silver"]
