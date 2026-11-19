"""Os dados fictícios precisam trazer os problemas que o pipeline deve resolver."""
import csv

from dados.gerar import gerar


def _ler(arquivo):
    with open(arquivo, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def test_gera_dois_lotes_sempre_iguais(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    assert gerar(a) == gerar(b)
    assert (a / "os_lote1.csv").read_bytes() == (b / "os_lote1.csv").read_bytes()


def test_lote1_tem_os_defeitos_conhecidos(tmp_path):
    linhas = _ler(gerar(tmp_path) and tmp_path / "os_lote1.csv")
    numeros = [l["numero_os"] for l in linhas]
    assert "" in numeros                                   # OS sem número
    assert {"OS-90001", "OS-90002", "OS-90003", "OS-90004", "OS-90005"} <= set(numeros)
    assert len(numeros) - len(set(numeros)) >= 8           # linhas duplicadas
    equipamentos = {l["equipamento"] for l in linhas}
    assert {"MC-99"} <= equipamentos
    assert any(e.lower().startswith("monta") for e in equipamentos)  # escrito por extenso
    assert any("-" in l["descricao"] for l in linhas)                # descrição com hífen (bug do TCC)


def test_lote2_atualiza_os_do_lote1(tmp_path):
    gerar(tmp_path)
    lote1 = {l["numero_os"]: l for l in _ler(tmp_path / "os_lote1.csv")}
    lote2 = _ler(tmp_path / "os_lote2.csv")
    atualizadas = [l for l in lote2 if l["numero_os"] in lote1]
    assert atualizadas, "o lote 2 precisa atualizar alguma OS do lote 1"
    assert all(lote1[l["numero_os"]]["status"] == "Aberta" and l["status"] == "Fechada" for l in atualizadas)
