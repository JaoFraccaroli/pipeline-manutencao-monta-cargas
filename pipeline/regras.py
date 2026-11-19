"""Regras de limpeza da camada Silver, como expressões nativas do Spark (sem UDF).

Cada função recebe uma coluna e devolve a coluna limpa, ou NULO quando não dá para
entender o valor. O nulo é o sinal para a Silver mandar a linha para a quarentena.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

FORMATOS_DATA_HORA = ["dd/MM/yyyy HH:mm", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss"]
FORMATOS_SO_DATA = ["dd/MM/yyyy", "dd-MM-yyyy", "yyyy-MM-dd"]


def _sem_acento(c: Column) -> Column:
    return F.translate(c, "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
                       "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC")


def normalizar_equipamento(c: Column) -> Column:
    """'mc01', 'MC 1', ' MC-01 ', 'Monta-cargas 01', 'monta cargas 1' -> 'MC-01'."""
    texto = F.upper(F.trim(_sem_acento(c)))
    texto = F.regexp_replace(texto, r"^MONTA[\s-]*CARGAS?", "MC")
    numero = F.regexp_extract(texto, r"^MC\s*-?\s*0*(\d{1,2})$", 1)
    return F.when(numero != "", F.concat(F.lit("MC-"), F.lpad(numero, 2, "0")))


def converter_data(c: Column) -> Column:
    """Tenta vários formatos; data impossível (31/02) ou texto vira nulo."""
    texto = F.trim(c)
    tentativas = [F.to_timestamp(texto, f) for f in FORMATOS_DATA_HORA + FORMATOS_SO_DATA]
    return F.when(texto != "", F.coalesce(*tentativas))


def tem_hora(c: Column) -> Column:
    """True se o texto trazia hora (ex.: '05/03/2026 14:30'); False se era só a data."""
    return F.trim(c).rlike(r"\d{1,2}:\d{2}")


def normalizar_tipo(c: Column) -> Column:
    t = F.lower(F.trim(_sem_acento(c)))
    return (
        F.when(t.rlike(r"^corr"), "corretiva")
        .when(t.rlike(r"^prev"), "preventiva")
        .when(t.rlike(r"^pred"), "preditiva")
    )


def converter_horas(c: Column) -> Column:
    """'1,5' / '1.5' / '2h' -> 1.5 / 2.0;  '90min' -> 1.5;  '1h30' -> 1.5."""
    t = F.regexp_replace(F.lower(F.trim(c)), r"\s+", "")
    decimal = F.regexp_replace(F.regexp_replace(t, ",", "."), "h$", "")
    return (
        F.when(t.rlike(r"^\d+([.,]\d+)?h?$"), decimal.cast("double"))
        .when(t.rlike(r"^\d+min$"), F.regexp_extract(t, r"^(\d+)", 1).cast("double") / 60)
        .when(t.rlike(r"^\d+h\d{1,2}$"),
              F.regexp_extract(t, r"^(\d+)h", 1).cast("double")
              + F.regexp_extract(t, r"h(\d{1,2})$", 1).cast("double") / 60)
    )
