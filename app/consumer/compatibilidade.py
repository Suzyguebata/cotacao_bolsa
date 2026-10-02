import logging
import sys
from typing import Iterable, List, Sequence


def verificar_compatibilidade(
    colunas_existentes: Iterable[str],
    particoes_existentes: Sequence[str],
    colunas_esperadas: Iterable[str],
    particoes_esperadas: Sequence[str],
) -> List[str]:
    """Compara o layout de uma tabela Delta já gravada com o que o job vai escrever.

    Retorna a lista de divergências (vazia quando compatível).
    """
    problemas = []
    if list(particoes_existentes) != list(particoes_esperadas):
        problemas.append(
            f"particionamento divergente: existente={list(particoes_existentes)} esperado={list(particoes_esperadas)}"
        )

    existentes = set(colunas_existentes)
    esperadas = set(colunas_esperadas)
    ausentes = sorted(esperadas - existentes)
    obsoletas = sorted(existentes - esperadas)
    if ausentes:
        problemas.append(f"colunas ausentes na tabela existente: {ausentes}")
    if obsoletas:
        problemas.append(f"colunas obsoletas na tabela existente: {obsoletas}")
    return problemas


def garantir_tabela_compativel(spark, caminho, df_saida, particoes_esperadas, logger, log_event):
    """Interrompe o job com erro explícito se a tabela de destino tiver layout antigo.

    Sem isso, o stream só falharia no primeiro micro-batch com erro genérico do Delta.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, caminho):
        return

    tabela = DeltaTable.forPath(spark, caminho)
    particoes_existentes = tabela.detail().select("partitionColumns").first()[0]
    colunas_existentes = tabela.toDF().columns

    problemas = verificar_compatibilidade(
        colunas_existentes, particoes_existentes, df_saida.columns, particoes_esperadas
    )
    if not problemas:
        log_event(logger, logging.INFO, "delta_table_schema_compatible", caminho=caminho)
        return

    log_event(
        logger,
        logging.ERROR,
        "delta_table_schema_incompatible",
        caminho=caminho,
        problemas=problemas,
        acao="Execute ./reset_pipeline.sh para recriar o data lake com o layout atual.",
    )
    print(f"[ERRO] Tabela Delta em {caminho} incompatível com o layout atual: {problemas}")
    print("[ERRO] Execute ./reset_pipeline.sh e suba o pipeline novamente.")
    sys.exit(1)
