import pandas as pd

def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Parsea tipos sin romper: fecha→datetime, monto→numérico."""
    out = df.copy()
    if "fecha" in out.columns:
        out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce")
    if "monto" in out.columns:
        out["monto"] = pd.to_numeric(out["monto"], errors="coerce")
    return out

def clean_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza simple:
    - quita duplicados por venta_id (si existe)
    - pone montos negativos en 0 (ejemplo de política)
    """
    out = df.copy()
    if "venta_id" in out.columns:
        out = out.drop_duplicates(subset=["venta_id"])
    if "monto" in out.columns:
        out.loc[out["monto"] < 0, "monto"] = 0
    return out
