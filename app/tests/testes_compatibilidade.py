from consumer.compatibilidade import verificar_compatibilidade


COLUNAS = ["ticket_ativo_b3", "valor_atual", "data_hora_atualizacao_valor", "ano_mes_dia"]
PARTICOES = ["ticket_ativo_b3", "ano_mes_dia"]


def test_layout_identico_e_compativel():
    assert verificar_compatibilidade(COLUNAS, PARTICOES, COLUNAS, PARTICOES) == []


def test_detecta_particionamento_antigo():
    problemas = verificar_compatibilidade(COLUNAS, ["ticket_ativo_b3", "data"], COLUNAS, PARTICOES)

    assert len(problemas) == 1
    assert "particionamento divergente" in problemas[0]


def test_detecta_colunas_renomeadas():
    colunas_antigas = ["ticket_ativo_b3", "valor_atual", "data_hora_evento", "ano_mes_dia"]

    problemas = verificar_compatibilidade(colunas_antigas, PARTICOES, COLUNAS, PARTICOES)

    assert any("ausentes" in p and "data_hora_atualizacao_valor" in p for p in problemas)
    assert any("obsoletas" in p and "data_hora_evento" in p for p in problemas)
