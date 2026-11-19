import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark", reason="PySpark não instalado")
    from pipeline.sessao import criar_spark
    sessao = criar_spark("testes")
    yield sessao
    sessao.stop()
