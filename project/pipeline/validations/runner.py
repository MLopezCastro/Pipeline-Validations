# project/pipeline/validations/runner.py
from .syntactic import check_column_names, check_types
from .semantic import check_positive, check_not_future_dates
from .statistical import validate_statistical  # si no lo tenés, hacé que retorne []

def run_validations(df):
    """
    Orquesta TODAS las validaciones y devuelve una lista de strings (errores).
    No rompe nada de lo previo: usa tus funciones existentes.
    """
    errors: list[str] = []

    # ---- Config mínima para este dataset (ajustá si querés) ----
    expected_cols = ["fecha", "monto"]  # agregá otras si tu CSV las tiene
    expected_types = {
        # OJO: si 'fecha' viene como texto, podés dejarla como 'object' por ahora
        # "fecha": "datetime64[ns]",
        "monto": "float64"  # si en tu CSV es int, cambiá a "int64"
    }

    # ---- SINTÁCTICAS ----
    errors.extend(check_column_names(df, expected_cols))
    # Types: evitamos explotar si no está la columna
    try:
        errors.extend(check_types(df, {k: v for k, v in expected_types.items() if k in df.columns}))
    except Exception as ex:
        errors.append(f"❌ Error en check_types: {ex}")

    # ---- SEMÁNTICAS ----
    errors.extend(check_positive(df, "monto"))
    errors.extend(check_not_future_dates(df, "fecha"))

    # ---- ESTADÍSTICAS (placeholder) ----
    try:
        found = validate_statistical(df) if 'validate_statistical' in globals() else []
        errors.extend(found or [])
    except Exception as ex:
        errors.append(f"❌ Error en validate_statistical: {ex}")

    return [str(e) for e in errors]
