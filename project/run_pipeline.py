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
GOLD_DIR    = DATA / "gold"
GOLD_PATH   = GOLD_DIR / "ventas_gold.csv"

SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
QUAR_PATH.parent.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

def route_to_silver_quarantine(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reglas fila-a-fila:
      - venta_id, cliente_id, producto_id, fecha, monto, moneda: no nulos
      - fecha parseable y NO futura
      - monto numérico >= 0 y no outlier grosero (> 1e7)
      - moneda en {"ARS","USD"}
    Cumple -> silver; no cumple -> quarantine.
    """
    df2 = df.copy()

    # Tipificar básico
    df2["monto"] = pd.to_numeric(df2.get("monto"), errors="coerce")
    df2["fecha"] = pd.to_datetime(df2.get("fecha"), errors="coerce")

    required_cols = ["venta_id", "cliente_id", "producto_id", "fecha", "monto", "moneda"]
    for c in required_cols:
        if c not in df2.columns:
            df2[c] = pd.NA

    valid_moneda = {"ARS", "USD"}
    today = pd.Timestamp(datetime.now().date())

    mask_invalid = (
        df2["venta_id"].isna()
        | df2["cliente_id"].isna()
        | df2["producto_id"].isna()
        | df2["fecha"].isna()
        | (df2["fecha"] > today)
        | df2["monto"].isna()
        | (df2["monto"] < 0)
        | (df2["monto"] > 10_000_000)
        | ~df2["moneda"].isin(valid_moneda)
    )

    df_silver = df2[~mask_invalid].copy()
    df_quar   = df2[mask_invalid].copy()

    # Ordenar columnas (cosmético)
    ordered_cols = [c for c in required_cols if c in df2.columns]
    other_cols = [c for c in df2.columns if c not in ordered_cols]
    df_silver = df_silver[ordered_cols + other_cols]
    df_quar   = df_quar[ordered_cols + other_cols]

    return df_silver, df_quar

def main():
    # 1) Cargar datos (si hay encabezados raros, pandas igual los toma como strings)
    logger.info(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)

    # 2) Validaciones (para observabilidad)
    errors = run_validations(df)
    logger.info(f"Validaciones ejecutadas. Errores detectados: {len(errors)}")
    for e in errors:
        logger.warning(e)

    # 3) Reportes (4.5 y 4.6)
    save_validation_report(errors, output_path=str(LOGS / "validation_report.csv"))
    save_json_log(errors,      output_path=str(LOGS / "validation_report.json"))
    logger.info(f"Reportes escritos en: {LOGS}")

    # 4) Routing a SILVER / QUARANTINE
    df_silver, df_quar = route_to_silver_quarantine(df)
    df_silver.to_csv(SILVER_PATH, index=False)
    df_quar.to_csv(QUAR_PATH, index=False)
    logger.info(f"SILVER actualizado: {SILVER_PATH} (rows={len(df_silver)})")
    logger.info(f"QUARANTINE actualizado: {QUAR_PATH} (rows={len(df_quar)})")

    # 5) GOLD (curado para BI) — idempotente: se sobreescribe en cada run
    df_gold = df_silver.copy()
    df_gold.to_csv(GOLD_PATH, index=False)
    logger.info(f"GOLD actualizado: {GOLD_PATH} (rows={len(df_gold)})")

    # (Opcional) derivados GOLD para BI (agregados rápidos)
    try:
        # diario por moneda
        df_silver["fecha"] = pd.to_datetime(df_silver["fecha"], errors="coerce")
        gold_daily = (
            df_silver.dropna(subset=["fecha"])
                     .groupby([df_silver["fecha"].dt.date, "moneda"], as_index=False)["monto"]
                     .sum().rename(columns={"fecha": "dia", "monto":"monto_total"})
        )
        (GOLD_DIR / "ventas_gold_daily.csv").write_text(
            gold_daily.to_csv(index=False), encoding="utf-8"
        )

        # por cliente y moneda
        gold_cliente = (
            df_silver.groupby(["cliente_id","moneda"], as_index=False)["monto"]
                     .sum().rename(columns={"monto":"monto_total"})
        )
        (GOLD_DIR / "ventas_gold_by_cliente.csv").write_text(
            gold_cliente.to_csv(index=False), encoding="utf-8"
        )

        logger.info(
            f"GOLD derivados: daily={len(gold_daily)} filas, by_cliente={len(gold_cliente)} filas"
        )
    except Exception as ex:
        logger.warning(f"No pude generar KPIs GOLD opcionales: {ex}")

    logger.info("Ejecución finalizada.")

if __name__ == "__main__":
    main()
