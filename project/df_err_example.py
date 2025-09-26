import pandas as pd
from pipeline.validations.runner import run_validations

df_err = pd.DataFrame({
    "venta_id":   [1,2,3,4,5,6,7,8,9,10],
    "fecha":      ["2025-07-14", None, "2025-07-14", "2099-01-01",
                   "2025-07-15", "2025-07-15", "2025-07-16", "2025-07-16",
                   "2025-07-17", "2025-07-17"],
    "cliente_id": [1001,1002,1002,1003,1004,1005,1001,1002,1003,1004],
    "monto":      [150, 200, -20, 999999.9, 0, 85.5, -1, 42, 13.2, 7.7],
    "moneda":     ["ARS","ARS","USD","ARS","XXX","USD","ARS","ARS","USD","ARS"]
})

run_validations(df_err)
