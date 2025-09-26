from __future__ import annotations
import logging
import pandas as pd
from typing import List
from . import syntactic, semantic, statistical

# logging (archivo + consola)
def _setup_logging():
    logging.basicConfig(
        filename="project/logs/validation_log.txt",
        level=logging.INFO,
        format="%(levelname)s | %(message)s",
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root = logging.getLogger()
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(console)

_setup_logging()

def log_errors(errors: List[str]) -> None:
    for e in errors:
        logging.warning(e)

def run_validations(df: pd.DataFrame) -> None:
    errors: List[str] = []

    # Sintácticas
    errors += syntactic.check_column_names(
        df, ["venta_id","fecha","cliente_id","producto_id","monto","moneda"]
    )
    errors += syntactic.check_types(df, {
        "venta_id": "int64",
        "fecha": "object",
        "cliente_id": "int64",
        "producto_id": "int64",
        "monto": "float64",
        "moneda": "object",
    })

    # Semánticas
    errors += semantic.check_positive(df, "monto")
    errors += semantic.check_not_future_dates(df, "fecha")
    errors += semantic.check_values_in_domain(df, "moneda", ["ARS","USD","EUR"])

    # Estadísticas
    errors += statistical.check_row_count(df, 1, 1_000_000)

    if errors:
        log_errors(errors)
        print("\n".join(errors))
    else:
        msg = "✅ Todos los chequeos pasaron"
        logging.info(msg)
        print(msg)
