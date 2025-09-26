from __future__ import annotations
import pandas as pd
from typing import List

# ---- C. Validaciones ESTADÍSTICAS (conteos, IQR, etc.) ----

def check_row_count(df: pd.DataFrame, min_expected: int, max_expected: int) -> List[str]:
    """
    Revisa que la cantidad de filas esté en un rango razonable.
    """
    n = len(df)
    if n < min_expected or n > max_expected:
        return [f"❌ Cantidad de filas inesperada: {n} (esperado entre {min_expected}-{max_expected})"]
    return []

def iqr_outlier_mask(series: pd.Series, k: float = 1.5) -> pd.Series:
    """
    Devuelve una máscara booleana con outliers por IQR.
    """
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - k * iqr, q3 + k * iqr
    return (series < lo) | (series > hi)
