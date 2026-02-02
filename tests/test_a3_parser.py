from src import a3_validator


def test_parse_a3_error_log_handles_lines():
    log_text = """
    Línea 3: Cuenta 700000 no existe
    Linea 5 -> Diario INV inválido
    Error general sin linea
    """
    errors = a3_validator.parse_a3_error_log(log_text)
    assert ("Cuenta" in errors[0][1] or errors[0][1] == "Cuenta") and errors[0][0] == 3
    assert errors[1][0] == 5
    assert errors[-1][0] == 0  # general fallback
