import datetime as dt

import pytest

REF = dt.date(2026, 11, 19)
COLS = "numero_os string, equipamento string, tipo string, data_abertura string, data_fechamento string, " \
       "horas_paradas string, descricao string, tecnico string, status string, arquivo_origem string, " \
       "carregado_em timestamp, linha_no_arquivo long"
T1, T2 = dt.datetime(2026, 11, 1, 3), dt.datetime(2026, 11, 2, 3)


def linha(numero="OS-00001", eq="MC-01", tipo="Corretiva", abre="05/03/2026 14:30", fecha="05/03/2026 16:00",
          horas="1,5", desc="Lâmpada do MC-02 queimada", status="Fechada", arquivo="lote1.csv", quando=T1, pos=0):
    return (numero, eq, tipo, abre, fecha, horas, desc, "Técnico A", status, arquivo, quando, pos)


@pytest.fixture
def processar(spark):
    from pipeline.silver import limpar, separar

    def _processar(*linhas):
        aprovadas, rejeitadas = separar(limpar(spark.createDataFrame(list(linhas), COLS), REF))
        return ({r.numero_os: r.asDict() for r in aprovadas.collect()},
                {(r.numero_os or "(vazio)"): r.motivo for r in rejeitadas.collect()})
    return _processar


def test_linha_boa_passa_limpa(processar):
    ok, rej = processar(linha(eq="monta cargas 1", tipo="corr.", horas="90min"))
    assert rej == {}
    r = ok["OS-00001"]
    assert (r["equipamento"], r["tipo"], r["horas_paradas"]) == ("MC-01", "corretiva", 1.5)


def test_descricao_com_hifen_fica_inteira(processar):
    """No TCC, 'Lâmpada do MC-02 queimada' virava 'Lâmpada do MC'."""
    ok, _ = processar(linha(desc="Lâmpada do MC-02 queimada"))
    assert ok["OS-00001"]["descricao"] == "Lâmpada do MC-02 queimada"


@pytest.mark.parametrize("campos, motivo", [
    ({"numero": ""}, "número da OS vazio"),
    ({"eq": "MC-99"}, "equipamento desconhecido: MC-99"),
    ({"tipo": "Melhoria"}, "tipo de manutenção desconhecido"),
    ({"abre": "31/02/2026 10:00"}, "data de abertura inválida: 31/02/2026 10:00"),
    ({"fecha": "amanhã"}, "data de fechamento inválida: amanhã"),
    ({"abre": "10/06/2026 10:00", "fecha": "09/06/2026 08:00"}, "fechamento antes da abertura"),
    ({"abre": "15/03/2031 09:00", "fecha": ""}, "data de abertura no futuro"),
    ({"horas": "duas"}, "horas paradas inválidas: duas"),
])
def test_linha_ruim_vai_para_quarentena_com_motivo(processar, campos, motivo):
    ok, rej = processar(linha(**campos))
    assert ok == {}
    assert motivo in list(rej.values())[0]


def test_varios_motivos_na_mesma_linha(processar):
    _, rej = processar(linha(eq="XX", tipo="?"))
    assert rej["OS-00001"] == "equipamento desconhecido: XX; tipo de manutenção desconhecido"


def test_data_so_com_dia_compara_por_dia(processar):
    """'07/04 01:46' -> '07-04-2026' (sem hora) NÃO é fechar antes de abrir."""
    ok, rej = processar(linha(abre="07/04/2026 01:46", fecha="07-04-2026"))
    assert rej == {} and "OS-00001" in ok


def test_os_aberta_sem_fechamento_e_valida(processar):
    ok, rej = processar(linha(fecha="", horas="", status="aberta"))
    assert rej == {} and ok["OS-00001"]["status"] == "Aberta" and ok["OS-00001"]["data_fechamento"] is None


def test_duplicatas_viram_uma_so(processar):
    ok, _ = processar(linha(pos=1), linha(pos=2), linha(pos=3))
    assert len(ok) == 1


def test_versao_mais_recente_vence(processar):
    aberta = linha(fecha="", horas="", status="Aberta", arquivo="lote1.csv", quando=T1)
    fechada = linha(status="Fechada", arquivo="lote2.csv", quando=T2)
    ok, _ = processar(fechada, aberta)  # ordem de chegada não importa
    assert ok["OS-00001"]["status"] == "Fechada" and ok["OS-00001"]["arquivo_origem"] == "lote2.csv"
