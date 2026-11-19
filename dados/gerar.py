"""Gera ordens de serviço FICTÍCIAS de manutenção de monta-cargas, com os problemas
típicos de dados reais (os mesmos que apareceram no TCC):

- equipamento escrito de vários jeitos: "MC-01", "mc01", "MC 1", "Monta-cargas 01"
- datas em formatos diferentes: "05/03/2026 14:30", "2026-03-05 14:30:00", "05-03-2026"
- tipo escrito de vários jeitos: "Corretiva", "CORRETIVA", "corr.", "prev", "Preditiva"
- horas paradas como "1,5", "2h", "90min", "1h30"
- descrições com hífen ("Lâmpada do MC-02 queimada"), que o TCC cortava
- defeitos para a camada Silver barrar: linhas duplicadas, número da OS vazio,
  equipamento inexistente, data impossível, fechamento antes da abertura

Gera dois lotes (dois "dias" de carga): o lote 2 traz OS novas e ATUALIZA OS do lote 1
(ex.: uma OS aberta que foi fechada), para testar a carga incremental.

    python dados/gerar.py            (grava em dados/entrada/)
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEMENTE = 2026
EQUIPAMENTOS = ["MC-01", "MC-02", "MC-03", "MC-04", "MC-05", "MC-06"]
# alguns equipamentos falham mais (o pipeline deve mostrar isso no Gold)
PESO_FALHA = {"MC-01": 1.0, "MC-02": 2.5, "MC-03": 1.0, "MC-04": 0.6, "MC-05": 1.4, "MC-06": 0.8}
TECNICOS = ["Técnico A", "Técnico B", "Técnico C", "Técnico D"]
PROBLEMAS = [
    "Lâmpada do {eq} queimada",
    "Corrente de tração do {eq} com folga",
    "Vazamento de óleo no cilindro - {eq}",
    "Botoeira de emergência travada",
    "Sensor de fim de curso com mau contato",
    "Ruído no motor de elevação",
    "Cabo de aço desfiado - troca preventiva",
    "Porta de pavimento não fecha",
    "Freio do motor desregulado",
    "Inversor de frequência em falha (E-07)",
]
COLUNAS = ["numero_os", "equipamento", "tipo", "data_abertura", "data_fechamento",
           "horas_paradas", "descricao", "tecnico", "status"]


def escrever_equipamento(eq: str, r: random.Random) -> str:
    n = int(eq.split("-")[1])
    return r.choice([eq, eq, eq, eq.lower().replace("-", ""), f"MC {n}", f"Monta-cargas {n:02d}",
                     f" {eq} ", f"monta cargas {n}"])


def escrever_tipo(tipo: str, r: random.Random) -> str:
    opcoes = {
        "corretiva": ["Corretiva", "CORRETIVA", "corretiva", "corr.", "Corretiva "],
        "preventiva": ["Preventiva", "PREVENTIVA", "prev", "Prev."],
        "preditiva": ["Preditiva", "PREDITIVA", "pred."],
    }
    return r.choice(opcoes[tipo])


def escrever_data(d: datetime, r: random.Random) -> str:
    return r.choice([
        d.strftime("%d/%m/%Y %H:%M"), d.strftime("%d/%m/%Y %H:%M"),
        d.strftime("%Y-%m-%d %H:%M:%S"), d.strftime("%d/%m/%Y"), d.strftime("%d-%m-%Y"),
    ])


def escrever_horas(h: float, r: random.Random) -> str:
    minutos = int(round(h * 60))
    inteiras, resto = divmod(minutos, 60)
    opcoes = [f"{h:.1f}".replace(".", ","), f"{h:.1f}", f"{minutos}min"]
    if resto == 0:
        opcoes.append(f"{inteiras}h")
    else:
        opcoes.append(f"{inteiras}h{resto:02d}")
    return r.choice(opcoes)


def gerar_os(numero: int, r: random.Random, inicio: datetime, dias: int) -> dict:
    eq = r.choices(EQUIPAMENTOS, weights=[PESO_FALHA[e] for e in EQUIPAMENTOS])[0]
    tipo = r.choices(["corretiva", "preventiva", "preditiva"], weights=[6, 3, 1])[0]
    abertura = inicio + timedelta(days=r.uniform(0, dias), minutes=r.randint(0, 600))
    horas = round(r.choice([0.5, 1, 1.5, 2, 3, 4, 6, 8]) * (1.6 if tipo == "corretiva" else 0.8), 1)
    fechamento = abertura + timedelta(hours=horas + r.uniform(0, 3))
    aberta = r.random() < 0.05
    return {
        "numero_os": f"OS-{numero:05d}",
        "equipamento": escrever_equipamento(eq, r),
        "tipo": escrever_tipo(tipo, r),
        "data_abertura": escrever_data(abertura, r),
        "data_fechamento": "" if aberta else escrever_data(fechamento, r),
        "horas_paradas": "" if aberta else escrever_horas(horas, r),
        "descricao": r.choice(PROBLEMAS).format(eq=eq),
        "tecnico": r.choice(TECNICOS),
        "status": "Aberta" if aberta else "Fechada",
    }


def com_defeitos(linhas: list[dict], r: random.Random) -> list[dict]:
    """Insere defeitos conhecidos, que a camada Silver precisa barrar ou tratar."""
    ruins = []
    base = r.sample(linhas, 6)
    ruins.append({**base[0], "numero_os": ""})                                   # sem número
    ruins.append({**base[1], "numero_os": "OS-90001", "equipamento": "MC-99"})   # equipamento inexistente
    ruins.append({**base[2], "numero_os": "OS-90002", "data_abertura": "31/02/2026 10:00"})  # data impossível
    ruins.append({**base[3], "numero_os": "OS-90003", "data_abertura": "10/06/2026 10:00",
                  "data_fechamento": "09/06/2026 08:00"})                         # fecha antes de abrir
    ruins.append({**base[4], "numero_os": "OS-90004", "tipo": "Melhoria"})       # tipo desconhecido
    ruins.append({**base[5], "numero_os": "OS-90005", "data_abertura": "15/03/2031 09:00",
                  "data_fechamento": ""})                                         # data no futuro
    duplicadas = [dict(l) for l in r.sample(linhas, 8)]                          # linhas repetidas
    return linhas + ruins + duplicadas


def gerar(pasta: Path = Path(__file__).parent / "entrada") -> dict[str, int]:
    r = random.Random(SEMENTE)
    pasta.mkdir(parents=True, exist_ok=True)
    lote1 = [gerar_os(n, r, datetime(2026, 1, 1, 7), 240) for n in range(1, 601)]
    lote1 = com_defeitos(lote1, r)

    # Lote 2: OS novas + atualização de OS que estavam abertas no lote 1
    novas = [gerar_os(n, r, datetime(2026, 8, 29, 7), 60) for n in range(601, 741)]
    atualizadas = []
    for l in lote1:
        if l["status"] == "Aberta" and l["numero_os"].startswith("OS-0"):
            atualizadas.append({**l, "status": "Fechada", "data_fechamento": "", "horas_paradas": "2h"})
    for a in atualizadas:  # fechamento 1 dia depois da abertura, no mesmo formato da abertura
        a["data_fechamento"] = a["data_abertura"]
        a["observacao_carga"] = "fechada no lote 2"
    lote2 = novas + [{k: v for k, v in a.items() if k in COLUNAS} for a in atualizadas]

    contagem = {}
    for nome, linhas in [("os_lote1.csv", lote1), ("os_lote2.csv", lote2)]:
        r.shuffle(linhas)
        with open(pasta / nome, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLUNAS, delimiter=";")
            w.writeheader()
            w.writerows(linhas)
        contagem[nome] = len(linhas)
    return contagem


if __name__ == "__main__":
    for nome, n in gerar().items():
        print(f"{nome}: {n} linhas")
