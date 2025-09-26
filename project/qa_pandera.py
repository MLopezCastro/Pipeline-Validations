from pathlib import Path
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

REPORT_DIR = Path("project") / "logs"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

DATE_MIN = pd.Timestamp("2020-01-01")
DATE_MAX = pd.Timestamp("2030-12-31")

schema = DataFrameSchema(
    {
        "venta_id":   Column(int,   [Check.ge(1)], nullable=False, coerce=True, unique=True),
        "fecha":      Column(pa.DateTime, [
                            Check.ge(DATE_MIN),
                            Check.le(DATE_MAX),
                        ], nullable=False, coerce=True),
        "cliente_id": Column(int,   [Check.ge(1)], nullable=False, coerce=True),
        "producto_id":Column(int,   [Check.ge(1)], nullable=False, coerce=True),
        "monto":      Column(float, [Check.ge(0), Check.le(1_000_000)], nullable=False, coerce=True),
        "moneda":     Column(str,   Check.isin({"ARS","USD"}), nullable=False, coerce=True),
    },
    strict=False,  # permite columnas extra si aparecieran
)

def run_checkpoint(csv_path: str) -> bool:
    df = pd.read_csv(csv_path, encoding="utf-8")

    # --- Preprocesamiento (normalización ligera) ---
    # moneda en mayúsculas, trim espacios
    if "moneda" in df.columns:
        df["moneda"] = df["moneda"].astype(str).str.strip().str.upper()
    # parseo robusto de fechas (coerce -> NaT si es inválida)
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=False)

    try:
        schema.validate(df, lazy=True)  # acumula todos los fallos
        (REPORT_DIR / "qa_report.html").write_text(
            "<h2>✔️ Validación OK</h2><p>Todas las reglas pasaron.</p>",
            encoding="utf-8",
        )
        print("✔️ Validación OK")
        return True
    except pa.errors.SchemaErrors as err:
        failures = err.failure_cases

        # Adjuntar filas originales que fallaron (si hay índices)
        bad_idx = failures["index"].dropna()
        try:
            bad_idx = bad_idx.astype(int).unique()
            sample = df.loc[bad_idx]
        except Exception:
            sample = pd.DataFrame()

        failures.to_csv(REPORT_DIR / "qa_failures.csv", index=False, encoding="utf-8")
        html = (
            "<h2>❌ Validación FALLÓ</h2>"
            "<h3>Casos fallidos (Pandera)</h3>"
            + failures.to_html(index=False)
            + "<h3>Filas originales</h3>"
            + (sample.to_html(index=True) if not sample.empty else "<p>(sin filas)</p>")
        )
        (REPORT_DIR / "qa_report.html").write_text(html, encoding="utf-8")
        print("❌ Validación FALLÓ. Ver:", (REPORT_DIR / "qa_report.html").resolve())
        return False

if __name__ == "__main__":
    ok = run_checkpoint("data/bronze/ventas.csv")
    raise SystemExit(0 if ok else 1)

