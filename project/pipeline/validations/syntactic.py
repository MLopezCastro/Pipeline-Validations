from __future__ import annotations
import pandas as pd
from typing import Dict, List, Iterable, Union

# ---- A. Validaciones SINTÁCTICAS (forma/estructura) ----

def check_column_names(df: pd.DataFrame, expected: Iterable[str]) -> List[str]:
    """
    Verifica que existan todas las columnas esperadas (no modifica datos).
    """
    errors: List[str] = []
    expected_set = set(expected)
    missing = expected_set - set(df.columns)
    for col in sorted(missing):
        errors.append(f"❌ Falta la columna esperada: {col}")
    return errors

def check_types(df: pd.DataFrame, expected_types: Dict[str, Union[type, str]]) -> List[str]:
    """
    Verifica tipos por columna. Admite:
      - tipos de Python (int, float, str)
      - strings de dtype de pandas/numpy ('int64','float64','object','datetime64[ns]')
    Nota: no castea; sólo valida.
    """
    errors: List[str] = []
    for col, expected in expected_types.items():
        if col not in df.columns:
            errors.append(f"❌ Columna {col} ausente para validar tipos")
            continue

        s = df[col]

        if isinstance(expected, str):
            # validar por dtype de pandas
            if str(s.dtype) != expected:
                errors.append(f"❌ Columna {col} con dtype inesperado: {s.dtype} (se esperaba {expected})")
        else:
            # validar por tipo de elemento (rápido y simple)
            # nota: tolera nulos
            mask_non_null = s.notna()
            if not mask_non_null.any():
                continue
            # ejemplo: todos los no-nulos deben ser del tipo esperado
            ok = s[mask_non_null].map(type).eq(expected).all()
            if not ok:
                errors.append(f"❌ Columna {col} con tipo inesperado (se esperaba {expected.__name__})")
    return errors
