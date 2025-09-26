# project/run_pipeline.py
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

import pandas as pd
from pipeline.validations.runner import run_validations
from reporting import save_validation_report, save_json_log

# === Rutas robustas ===
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LOGS = ROOT / "project" / "logs"
LOGS.mkdir(parents=True, exist_ok=True)

# === Logging (consola + archivo) ===
LOG_FILE = LOGS / "pipeline.log"
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    ch = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)

# === Orígenes posibles del CSV ===
CANDIDATES = [DATA / "ventas.csv", DATA / "bronze" / "ventas.csv"]
csv_path = next((p for p in CANDIDATES if p.exists()), None)
if not csv_path:
    raise FileNotFoundError("No encontré el CSV. Probé:\n- " + "\n- ".join(map(str, CANDIDATES)))

# === Destinos ===
SILVER_PATH = DATA / "silver" / "ventas_silver.csv"
QUAR_PATH   = DATA / "quarantine" / "ventas_quarantine.csv"
SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
QUAR_PATH.parent.mkdir(parents=True, exist_ok=True)

def route_to_silver_quarantine(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reglas simples y claras:
    - Campos clave no nulos: venta_id, cliente_id, producto_id, fecha, monto, moneda
    - fecha: parseable y NO futura
    - monto: número y >= 0 (además, descartar outliers groseros > 1e7)
    - moneda: dentro del dominio {"ARS","USD"}
    Todo lo que no cumpla -> quarantine. El resto -> silver.
    """
    df2 = df.copy()

    # Tipificar
    df2["monto"] = pd.to_numeric(df2["monto"], errors="coerce")
    df2["fecha"] = pd.to_datetime(df2["fecha"], errors="coerce")

    required_cols = ["venta_id", "cliente_id", "producto_id", "fecha", "monto", "moneda"]
    for c in required_cols:
        if c not in df2.columns:
            df2[c] = pd.NA  # si faltó la columna, todo irá a quarantine (consistente)

    valid_moneda = {"ARS", "USD"}
    today = pd.Timestamp(datetime.now().date())

    # Reglas fila-a-fila
    mask_invalid = (
        df2["venta_id"].isna() |
        df2["cliente_id"].isna() |
        df2["producto_id"].isna() |
        df2["fecha"].isna() |
        (df2["fecha"] > today) |
        df2["monto"].isna() |
        (df2["monto"] < 0) |
        (df2["monto"] > 10_000_000) |     # outlier grosero -> quarantine
        ~df2["moneda"].isin(valid_moneda)
    )

    df_silver = df2[~mask_invalid].copy()
    df_quar   = df2[mask_invalid].copy()
    return df_silver, df_quar

def main():
    # 1) Cargar datos
    logger.info(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)

    # 2) Validaciones (para reporte/observabilidad)
    errors = run_validations(df)
    logger.info(f"Validaciones ejecutadas. Errores detectados: {len(errors)}")
    for e in errors:
        logger.warning(e)

    # 3) Reportes (4.5 y 4.6)
    save_validation_report(errors, output_path=str(LOGS / "validation_report.csv"))
    save_json_log(errors,      output_path=str(LOGS / "validation_report.json"))
    logger.info(f"Reportes escritos en: {LOGS}")

    # 4) Routing a SILVER / QUARANTINE (fila-a-fila)
    df_silver, df_quar = route_to_silver_quarantine(df)
    df_silver.to_csv(SILVER_PATH, index=False)
    df_quar.to_csv(QUAR_PATH, index=False)
    logger.info(f"SILVER actualizado: {SILVER_PATH} (rows={len(df_silver)})")
    logger.info(f"QUARANTINE actualizado: {QUAR_PATH} (rows={len(df_quar)})")

    logger.info("Ejecución finalizada.")

if __name__ == "__main__":
    main()
