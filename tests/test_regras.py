import datetime as dt

import pytest


@pytest.fixture(scope="module")
def avaliar(spark):
    """Aplica uma regra a uma lista de textos e devolve os resultados em Python."""
    def _avaliar(regra, valores):
        df = spark.createDataFrame([(v,) for v in valores], "v string")
        return [r[0] for r in df.select(regra(df.v)).collect()]
    return _avaliar


def test_equipamento_escrito_de_varios_jeitos(avaliar):
    from pipeline.regras import normalizar_equipamento
    entradas = ["MC-01", "mc01", "MC 1", " MC-01 ", "Monta-cargas 01", "monta cargas 1", "MONTA-CARGAS 04", "Mc-6"]
    assert avaliar(normalizar_equipamento, entradas) == ["MC-01"] * 6 + ["MC-04", "MC-06"]


def test_equipamento_que_nao_da_para_entender_vira_nulo(avaliar):
    from pipeline.regras import normalizar_equipamento
    assert avaliar(normalizar_equipamento, ["", "Empilhadeira 2", "MC", None]) == [None, None, None, None]


def test_datas_em_varios_formatos(avaliar):
    from pipeline.regras import converter_data
    r = avaliar(converter_data, ["05/03/2026 14:30", "2026-03-05 14:30:00", "05/03/2026", "05-03-2026", "2026-03-05"])
    assert r == [dt.datetime(2026, 3, 5, 14, 30), dt.datetime(2026, 3, 5, 14, 30),
                 dt.datetime(2026, 3, 5), dt.datetime(2026, 3, 5), dt.datetime(2026, 3, 5)]


def test_data_impossivel_ou_vazia_vira_nulo(avaliar):
    from pipeline.regras import converter_data
    assert avaliar(converter_data, ["31/02/2026 10:00", "ontem", "", None]) == [None, None, None, None]


def test_reconhece_se_a_data_tinha_hora(avaliar):
    from pipeline.regras import tem_hora
    assert avaliar(tem_hora, ["05/03/2026 14:30", "05/03/2026"]) == [True, False]


def test_tipo_de_manutencao(avaliar):
    from pipeline.regras import normalizar_tipo
    r = avaliar(normalizar_tipo, ["Corretiva", "CORRETIVA ", "corr.", "prev", "Preventiva", "PREDITIVA", "pred.", "Melhoria"])
    assert r == ["corretiva"] * 3 + ["preventiva"] * 2 + ["preditiva"] * 2 + [None]


@pytest.mark.parametrize("texto, horas", [("1,5", 1.5), ("1.5", 1.5), ("2h", 2.0), ("2", 2.0), ("90min", 1.5),
                                          ("1h30", 1.5), (" 3,2 ", 3.2), ("45 min", 0.75)])
def test_horas_paradas(avaliar, texto, horas):
    from pipeline.regras import converter_horas
    assert avaliar(converter_horas, [texto])[0] == pytest.approx(horas)


def test_horas_invalidas_viram_nulo(avaliar):
    from pipeline.regras import converter_horas
    assert avaliar(converter_horas, ["duas horas", "", "-1"]) == [None, None, None]
