
```markdown
# 🛠️ Pipeline Validations

Este proyecto implementa un **pipeline de datos local** con Python, que incluye:

- **ETL básico**: extracción, carga y transformación de datos en CSV.
- **Validaciones semánticas, sintácticas y estadísticas** sobre datasets.
- **Logging y reporting** en múltiples formatos (`.log`, `.csv`, `.json`, `.html`).
- **Data Quality con Pandera**: equivalente a un "checkpoint" al estilo Great Expectations, pero usando una librería más ligera.

---

## 📂 Estructura del proyecto

```

Pipeline Validations/
│
├── .venv/ / .venv_ge311/         # entornos virtuales (ignorar en Git)
├── data/                         # datasets en diferentes capas
│   ├── bronze/ventas.csv         # CSV bruto
│   ├── silver/                   # datos limpios
│   ├── gold/                     # datos transformados finales
│   └── quarantine/               # datos rechazados
│
├── project/
│   ├── logs/                     # registros y reportes
│   │   ├── pipeline.log
│   │   ├── validation_log.txt
│   │   ├── validation_report.csv
│   │   ├── validation_report.json
│   │   ├── qa_failures.csv       # errores detectados con Pandera
│   │   └── qa_report.html        # reporte visual de QA
│   │
│   ├── pipeline/                 # módulos principales
│   │   ├── extract.py            # lectura de datos
│   │   ├── load.py               # carga a capas
│   │   ├── transform.py          # transformaciones
│   │   ├── reporting.py          # funciones de logging/reportes
│   │   └── validations/          # reglas de validación
│   │       ├── semantic.py
│   │       ├── syntactic.py
│   │       ├── statistical.py
│   │       └── runner.py         # orquestador de validaciones
│   │
│   ├── run_pipeline.py           # script principal (ETL + validaciones)
│   └── qa_pandera.py             # checkpoint de Pandera (reglas de calidad)
│
└── requirements.txt              # dependencias

````

---

## ⚙️ Instalación

1. Clonar el repo:
   ```bash
   git clone <URL>
   cd Pipeline-Validations
````

2. Crear entorno virtual (ejemplo con Python 3.11):

   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   ```

3. Instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```

---

## ▶️ Ejecución del pipeline

Para correr el pipeline completo (ETL + validaciones):

```bash
python project/run_pipeline.py
```

Esto genera:

* Logs en `project/logs/pipeline.log`
* Reportes en `.csv` y `.json`

---

## ✅ Validaciones con Pandera

En `project/qa_pandera.py` se definen reglas de calidad sobre `data/bronze/ventas.csv`.

Ejemplo de schema:

```python
schema = DataFrameSchema(
    {
        "venta_id":   Column(int,   [Check.ge(1)], nullable=False, coerce=True, unique=True),
        "fecha":      Column(pa.DateTime, [Check.ge(pd.Timestamp("2020-01-01")),
                                           Check.le(pd.Timestamp("2030-12-31"))],
                             nullable=False, coerce=True),
        "cliente_id": Column(int,   [Check.ge(1)], nullable=False, coerce=True),
        "producto_id":Column(int,   [Check.ge(1)], nullable=False, coerce=True),
        "monto":      Column(float, [Check.ge(0), Check.le(1_000_000)], nullable=False, coerce=True),
        "moneda":     Column(str,   Check.isin({"ARS","USD"}), nullable=False, coerce=True),
    },
    strict=False,
)
```

Para ejecutar el checkpoint:

```bash
python project/qa_pandera.py
```

Resultados:

* ✔️ **OK** → se genera un HTML con validación verde.
* ❌ **Fallo** → se registran los errores en `qa_failures.csv` y `qa_report.html`.

---

## 📊 Ejemplo de salida (errores)

`qa_failures.csv`:

```csv
schema_context,column,check,check_number,failure_case,index
DataFrameSchema,,column_in_dataframe,,id_venta,
Column,monto,greater_than_or_equal_to(0),0,-20.0,2
Column,monto,greater_than_or_equal_to(0),0,-1.0,6
```

`qa_report.html`:

```html
<h2>❌ Validación FALLÓ</h2>
<p>Detalle de casos fallidos:</p>
<table border="1">
  <tr><th>column</th><th>check</th><th>failure_case</th></tr>
  <tr><td>monto</td><td>>=0</td><td>-20.0</td></tr>
  <tr><td>monto</td><td>>=0</td><td>-1.0</td></tr>
</table>
```

---

## 🛠️ Tecnologías usadas

* **Python** (3.11+)
* **pandas** – manipulación de datos
* **pandera** – validación declarativa de dataframes
* **logging** – logs de ejecución
* **matplotlib** – visualización de errores por día

---

## 🚀 Próximos pasos

* Integrar `qa_pandera.py` dentro de `run_pipeline.py` como un paso automático de QA.
* Extender validaciones a más columnas/reglas.
* Conectar con un Data Lake o base SQL real.

---


