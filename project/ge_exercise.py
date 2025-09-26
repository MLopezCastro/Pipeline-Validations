from pathlib import Path
import pandas as pd
import great_expectations as gx

ROOT = Path(__file__).resolve().parents[1]
csv_path = ROOT / "data" / "bronze" / "ventas.csv"
csv_path.parent.mkdir(parents=True, exist_ok=True)
if not csv_path.exists():
    pd.DataFrame({
        "id_venta": [1,2,3,4],
        "monto":    [100,200,-5,50],        # -5 para forzar fallo
        "estado":   ["OK","OK","ERROR","OK"],
    }).to_csv(csv_path, index=False, encoding="utf-8")

# 1) Contexto (persistente en gx/)
context = gx.get_context()

# 2) Datasource + asset (Pandas). Si existen, se reutilizan.
ds_name, asset_name = "local_pandas", "ventas_df_asset"
try:
    ds = context.sources.get(ds_name)
except Exception:
    ds = context.sources.add_pandas(name=ds_name)

try:
    asset = ds.get_asset(asset_name)
except Exception:
    asset = ds.add_dataframe_asset(name=asset_name)

# 3) BatchRequest desde DataFrame
df = pd.read_csv(csv_path, encoding="utf-8")
batch_request = asset.build_batch_request(dataframe=df)

# 4) Suite + 3 expectativas
suite_name = "ventas_suite"
validator = context.get_validator(
    batch_request=batch_request,
    create_expectation_suite_with_name=suite_name
)
validator.expect_column_values_to_not_be_null("id_venta")
validator.expect_column_values_to_be_between("monto", min_value=0)
validator.expect_column_values_to_be_in_set("estado", ["OK","WARN","ERROR"])

res = validator.validate()
print("Expectations success ->", res.success)

# 5) Checkpoint
cp_name = "ventas_checkpoint"
context.add_or_update_checkpoint(
    name=cp_name,
    validations=[{"batch_request": batch_request, "expectation_suite_name": suite_name}],
)
run = context.run_checkpoint(checkpoint_name=cp_name)
print("Checkpoint success ->", run.success)

# 6) Data Docs (HTML)
context.build_data_docs()
html = ROOT / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
print("ABRÍ ESTE HTML:", html.resolve())

