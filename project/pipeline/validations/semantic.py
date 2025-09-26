from __future__ import annotations
import pandas as pd
from typing import Iterable, List

# ---- B. Validaciones SEMÁNTICAS (significado/reglas de negocio) ----

def check_positive(df: pd.DataFrame, column: str) -> List[str]:
    """
    Revisa que no existan valores negativos en la columna (p.ej. 'monto').
    """
    if column not in df.columns:
        return [f"❌ Columna {column} ausente para validar positivos"]
    if pd.to_numeric(df[column], errors="coerce").lt(0).any():
        return [f"❌ Columna {column} contiene valores negativos"]
    return []

def check_values_in_domain(df: pd.DataFrame, column: str, valid_values: Iterable) -> List[str]:
    """
    Revisa que todos los valores pertenezcan a un dominio permitido (lista/cjto).
    """
    if column not in df.columns:
        return [f"❌ Columna {column} ausente para validar dominio"]
    valid = set(valid_values)
    s = df[column]
    if not s.isin(valid).all():
        return [f"❌ Valores fuera de dominio en '{column}'"]
    return []

def check_not_future_dates(df: pd.DataFrame, date_col: str) -> List[str]:
    """
    Revisa que no haya fechas futuras (requiere parseo previo o formato ISO).
    """
    if date_col not in df.columns:
        return [f"❌ Columna {date_col} ausente para validar fechas"]
    fechas = pd.to_datetime(df[date_col], errors="coerce")
    if (fechas > pd.Timestamp.today().normalize()).any():
        return [f"❌ Existen fechas futuras en '{date_col}'"]
    return []
