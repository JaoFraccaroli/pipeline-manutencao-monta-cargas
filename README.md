# Pipeline de ordens de serviço de manutenção de monta-cargas

[![Pipeline de ordens de serviço](https://github.com/JaoFraccaroli/pipeline-manutencao-monta-cargas/actions/workflows/pipeline.yml/badge.svg)](https://github.com/JaoFraccaroli/pipeline-manutencao-monta-cargas/actions/workflows/pipeline.yml)

Projeto de **engenharia de dados** com **PySpark** e **Delta Lake**, na **arquitetura medalhão** (Bronze → Silver → Gold). O pipeline pega o histórico bagunçado de ordens de serviço (OS) de manutenção de monta-cargas, limpa, padroniza, separa o que não dá para confiar, e entrega indicadores de manutenção: **MTBF, MTTR e a sugestão da próxima manutenção**.

O tema vem do meu TCC, que usa IA para sugerir a data de manutenção a partir do histórico de OS. Lá, a limpeza dos dados era um script que eu rodava na mão. Aqui ela virou um **pipeline automático, testado e que roda sozinho**. Todos os dados são **fictícios**.

## A ideia em uma analogia

Pense numa rede de água:

| Camada | Analogia | O que tem aqui |
|---|---|---|
| **Bronze** | Água da nascente, como chegou | Os CSVs exatamente como vieram, tudo como texto, mais de qual arquivo veio e quando chegou |
| **Silver** | Água tratada | Dados limpos e padronizados, sem duplicatas. O que não dá para confiar vai para a **quarentena**, com o motivo |
| **Gold** | Água engarrafada, pronta para beber | Indicadores por equipamento e por mês, prontos para relatório |

## Os problemas que o pipeline resolve

São os mesmos que encontrei nos dados reais do TCC, reproduzidos no [gerador de dados fictícios](dados/gerar.py):

| Problema | Exemplo | O que a Silver faz |
|---|---|---|
| Equipamento escrito de vários jeitos | `mc01`, `MC 1`, `Monta-cargas 01`, ` MC-01 ` | Padroniza para `MC-01` e confere no cadastro |
| Datas em formatos diferentes | `05/03/2026 14:30`, `2026-03-05 14:30:00`, `05-03-2026` | Tenta cada formato; data impossível (`31/02`) vai para a quarentena |
| Datas com precisões diferentes | Abre `07/04/2026 01:46`, fecha `07-04-2026` (sem hora) | Compara só o **dia**, para não acusar "fechou antes de abrir" à toa |
| Tipo escrito de vários jeitos | `CORRETIVA`, `corr.`, `prev`, `pred.` | Padroniza para corretiva, preventiva ou preditiva |
| Horas paradas em formatos diferentes | `1,5`, `2h`, `90min`, `1h30` | Converte tudo para horas decimais |
| Descrição com hífen | `Lâmpada do MC-02 queimada` | Mantém o texto inteiro (no TCC, ele era cortado em "Lâmpada do MC") |
| Linhas duplicadas | A mesma OS repetida no arquivo | Fica uma só |
| OS atualizada em outro arquivo | Aberta no lote 1, fechada no lote 2 | A versão mais recente vence (MERGE) |
| Dados impossíveis | Sem número, equipamento `MC-99`, data no futuro, fechamento antes da abertura | Quarentena, com o motivo de cada linha |

Nada é corrigido "no chute": quando o pipeline não consegue entender um valor, a linha vai para a quarentena com uma explicação, em vez de entrar errada nos indicadores.

## Resultado com os dados fictícios

| Camada | Linhas |
|---|---|
| Bronze (dado cru, 2 arquivos) | 777 |
| Silver (limpo, sem duplicatas) | 740 |
| Quarentena | 6, exatamente os 6 defeitos plantados no gerador |

**Gold: indicadores por equipamento**, ordenados do que falha com mais frequência:

| Equipamento | Setor | Corretivas | MTBF (dias) | Última falha | Próxima manutenção sugerida |
|---|---|---|---|---|---|
| MC-02 | Expedição | 147 | 2,0 | 22/10/2026 | 24/10/2026 |
| MC-05 | Produção | 75 | 4,0 | 28/10/2026 | 01/11/2026 |
| MC-06 | Produção | 65 | 4,5 | 23/10/2026 | 28/10/2026 |
| MC-01 | Expedição | 57 | 5,3 | 27/10/2026 | 01/11/2026 |
| MC-03 | Armazém | 55 | 5,4 | 26/10/2026 | 31/10/2026 |
| MC-04 | Armazém | 28 | 10,7 | 21/10/2026 | 01/11/2026 |

- **MTBF** (tempo médio entre falhas) é a média de dias entre uma corretiva e a seguinte do mesmo equipamento.
- **MTTR** (tempo médio de reparo) é a média de horas paradas nas corretivas, e fica nos indicadores mensais.
- A **próxima manutenção sugerida** é a última falha somada ao MTBF.
- O gerador faz o MC-02 falhar mais, de propósito, e o pipeline o colocou em primeiro, como deveria.

## Qualidade de dados: o pipeline para se algo estiver errado

Depois da Silver e da Gold, [`pipeline/qualidade.py`](pipeline/qualidade.py) confere se o dado faz sentido. Se qualquer regra falhar, o pipeline **para** e explica o problema, em vez de publicar indicador errado:

- **Silver:**
  - não pode estar vazia;
  - cada OS aparece uma vez só;
  - número da OS preenchido;
  - equipamento existe no cadastro;
  - tipo é conhecido;
  - data de abertura preenchida;
  - fechamento não é antes da abertura;
  - horas paradas entre 0 e 72.
- **Gold:**
  - o total de OS bate com a Silver;
  - as corretivas batem com a Silver;
  - há um registro por equipamento;
  - o MTBF não é negativo.

## Rodar duas vezes não duplica nada

Em produção, um pipeline roda todo dia, e às vezes roda de novo por engano. Aqui isso é seguro:
- A **Bronze** não carrega de novo um arquivo que já carregou.
- A **Silver** grava com **MERGE**: OS nova entra, OS existente é atualizada, e nada é duplicado.
- A **Gold** é recalculada inteira a partir da Silver.

O GitHub Actions **roda o pipeline duas vezes seguidas** e confere que os números são idênticos.

## Testes

São **45 testes** com PySpark local ([`tests/`](tests)):

| Arquivo | O que testa |
|---|---|
| `test_regras.py` | Cada regra de limpeza: equipamento, datas, tipo e horas, incluindo os valores que precisam virar nulo |
| `test_silver.py` | Quarentena com o motivo certo, duplicatas, versão mais recente, descrição com hífen, comparação por dia |
| `test_gold.py` | MTBF, MTTR e próxima manutenção calculados à mão e comparados com o resultado |
| `test_qualidade.py` | As verificações reprovam dados ruins e aprovam dados bons |
| `test_pipeline.py` | Pipeline de ponta a ponta, carga incremental (lote 1, depois lote 2), rodar duas vezes e parar quando a qualidade falha |
| `test_gerador.py` | Os dados fictícios trazem os defeitos que o pipeline precisa tratar |

Um dos testes pegou um problema clássico de dados: **fuso horário**. Eu tinha fixado o Spark no horário de Brasília, e na máquina do GitHub, que usa UTC, "05/03 00:00" voltava como "05/03 03:00". Agora o Spark usa o fuso do sistema, e os testes rodam no fuso de São Paulo.

## Rodar no seu computador

Precisa de **Java 17** e **Python 3.11**:

```bash
pip install -r requirements.txt
python -m pytest                                                     # testes
python dados/gerar.py                                                # (re)gera os dados fictícios
python -m pipeline.executar --entrada dados/entrada --lake lake --exportar saida
```

O lake (Delta Lake) fica em `lake/`, as tabelas Gold em CSV ficam em `saida/`, e o resumo fica em `relatorio.md`.

## Rodar no Databricks (Free Edition)

O [Databricks Free Edition](https://www.databricks.com/learn/free-edition) é gratuito, sem cartão de crédito, para uso pessoal e de estudo.

1. No Databricks, vá em **Workspace → Create → Git folder** e cole a URL deste repositório.
2. Abra [`databricks/executar_no_databricks.py`](databricks/executar_no_databricks.py) e clique em **Run all**. Ele usa **o mesmo código** de `pipeline/`, com os dados num Volume do Unity Catalog.
3. Para agendar, use **Schedule** e escolha, por exemplo, todo dia às 6h. O notebook vira um **Job**.

> Esse notebook ainda não foi testado no Databricks; o pipeline é testado no GitHub Actions, com PySpark e Delta Lake locais.

## No GitHub Actions

- A cada push: testes, e depois o pipeline rodado duas vezes.
- **Toda segunda-feira**, o pipeline roda sozinho (job agendado), como em produção.
- O resumo da execução aparece na página do Actions, e as tabelas Gold ficam disponíveis para download em CSV.

## Estrutura

```
dados/gerar.py            gerador das OS fictícias (com os defeitos plantados)
dados/entrada/            os dois lotes de CSV
pipeline/regras.py        regras de limpeza (funções nativas do Spark, sem UDF)
pipeline/bronze.py        carga crua, sem repetir arquivo
pipeline/silver.py        limpeza, quarentena, deduplicação e MERGE
pipeline/gold.py          indicadores mensais e por equipamento
pipeline/qualidade.py     verificações de qualidade de dados
pipeline/executar.py      roda tudo e gera o relatório
databricks/               notebook para rodar no Databricks
```
