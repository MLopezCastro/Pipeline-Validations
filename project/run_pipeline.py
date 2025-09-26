from __future__ import annotations
import pandas as pd
from pathlib import Path

from pipeline.extract import read_csv_from_bronze, write_csv_to_bronze
from pipeline.transform import normalize_types, clean_business_rules
from pipeline.load import write_silver, write_gold, write_quarantine
from pipeline.validations.runner import run_validations
from pipeline.validations.statistical import iqr_outlier_mask

BASE = Path(__file__).resolve().parents[0]  # project/
ROOT = BASE.parents[0]                      # raíz del repo

BRONZE = ROOT / "data" / "bronze"

def seed_example_if_needed():
    BRONZE.mkdir(parents=True, exist_ok=True)
    f = BRONZE / "ventas.csv"
    if f.exists():
        return
    df = pd.DataFrame({
        "venta_id":[1,2,3,3,5,6,7,8,9,10],
        "fecha":["2025-07-14", None, "2025-07-14", "2099-01-01",
                 "2025-07-15","2025-07-15","2025-07-16","2025-07-16",
                 "2025-07-17","2025-07-17"],
        "cliente_id":[1001,1002,1002,1003,1004,1005,1001,1002,1003,1004],
        "producto_id":[222,223,223,223,224,225,222,223,223,224],
        "monto":[150,200,-20,999999.9,0,85.5,-1,42,13.2,7.7],
        "moneda":["ARS","ARS","USD","ARS","XXX","USD","ARS","ARS","USD","ARS"]
    })
    write_csv_to_bronze(ROOT, df, "ventas.csv")

def main():
    seed_example_if_needed()

    # Extract
    df = read_csv_from_bronze(ROOT, "ventas.csv")

    # Transform
    df = normalize_types(df)
    df = clean_business_rules(df)

    # Validate (log + consola)
    run_validations(df)

    # Split outliers → quarantine
    mask_out = iqr_outlier_mask(df["monto"]) | (df["monto"] > 50_000)
    df_ok = df[~mask_out].copy()
    df_q  = df[mask_out].copy()

    # Load
    write_silver(ROOT, df_ok, "ventas_silver.csv")
    if not df_q.empty:
        write_quarantine(ROOT, df_q, "ventas_quarantine.csv")
    write_gold(ROOT, df_ok, "ventas_gold.csv")

    print("✅ Pipeline finalizado. Revisá data/silver, data/gold y data/quarantine.")

if __name__ == "__main__":
    main()
