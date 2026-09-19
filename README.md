# Enterprise Energy Analytics: Automated Multi-Format ETL Data Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white&style=flat-square)
![Pandas](https://img.shields.io/badge/Pandas-Data_Engineering-150458?logo=pandas&logoColor=white&style=flat-square)
![Apache Parquet](https://img.shields.io/badge/Apache_Parquet-Columnar_Storage-4084AC?style=flat-square)
![Status](https://img.shields.io/badge/Status-Completed-success?style=flat-square)
[![View Notebook](https://img.shields.io/badge/Jupyter-View_Notebook-F37626?logo=jupyter&style=flat-square)](energy_etl_pipeline.ipynb)

## 📌 Abstract
In the utility and energy sector, monitoring power generation capability, sector consumption, and regional pricing dynamics is essential for market forecasting, procurement, and regulatory compliance. Previously, analytics teams relied on manual quarterly data retrieval and ad-hoc cleaning workflows—a labor-intensive process requiring days of effort that introduced human error and created reporting latency.

This project delivers a modular, automated Extract, Transform, Load (ETL) data engineering pipeline built in **Python** using `pandas`. The pipeline ingests heterogeneous data formats (tabular `.csv`/`.parquet` files and deeply nested JSON structures), executes schema normalization, handles missing data, applies temporal string parsing, and exports standardized datasets across columnar and delimited storage targets.

---

## 📂 Data Architecture & Schema
The pipeline consolidates retail electricity sales metrics and power generation capability records.

### Core Sales Schema Definition
| Field | Type | Description |
| :--- | :--- | :--- |
| `period` | `str` | Raw observation timestamp encoding month and year |
| `stateid` | `str` | Two-letter state postal abbreviation |
| `stateDescription` | `str` | Full geographic state name |
| `sectorid` | `str` | Energy consumer sector identifier |
| `sectorName` | `str` | Consumer category classification (`residential`, `transportation`, etc.) |
| `price` | `float` | Retail electricity rate |
| `price-units` | `str` | Measurement unit denomination (e.g., cents per kilowatt-hour) |

---

## 🛠️ Pipeline Architecture & Implementation
The ETL architecture is modularized into discrete, reusable functions enforcing strict validation at each stage.

### 1. Multi-Format Extraction (`E`)
* **Tabular Ingestion**: `extract_tabular_data()` validates file extensions and loads either `.csv` or `.parquet` formats into memory via `pd.read_csv()` and `pd.read_parquet()`.
* **Semi-Structured Ingestion**: `extract_json_data()` reads raw JSON files and flattens nested hierarchical objects into a 2D relational structure using `pd.json_normalize()`.

### 2. Business Transformation Engine (`T`)
* **Data Cleaning**: Removes unpriced transactions inplace via `raw_data.dropna(subset=['price'])` to prevent skewed baseline revenue metrics.
* **Cohort Isolation**: Filters records strictly to key high-volume demand sectors: `residential` and `transportation`.
* **Temporal Decomposition**: Parses the composite `period` string using vectorized slice operations (`.str[:4]` for `month` and `.str[-2:]` for `year`) to enable time-series aggregation.
* **Projection**: Prunes redundant categorical metadata, outputting a lean dimensional structure: `['year', 'month', 'stateid', 'price', 'price-units']`.

### 3. Target Loading & Serialization (`L`)
* **Columnar & Delimited Sinks**: `load()` validates output destinations, serializing transformed data to `.parquet` (for compressed analytical query performance) or `.csv` (for downstream reporting tools) with index exclusion.

---

## 💻 Core Python Code

```python
import json
import pandas as pd


def extract_tabular_data(file_path: str) -> pd.DataFrame:
  """Extract data from a tabular file format with pandas validation."""
  if file_path.endswith('.csv'):
    return pd.read_csv(file_path)
  elif file_path.endswith('.parquet'):
    return pd.read_parquet(file_path)
  else:
    raise Exception(
        'Warning: Invalid file extension. Please try with .csv or .parquet!'
    )


def extract_json_data(file_path: str) -> pd.DataFrame:
  """Extract and flatten data from a nested JSON file."""
  with open(file_path, 'r') as file:
    data = json.load(file)
  return pd.json_normalize(data)


def transform_electricity_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame:
  """Transform electricity sales records by cleaning prices and parsing period."""
  raw_data.dropna(subset=['price'], inplace=True)
  raw_data = raw_data[
      raw_data['sectorName'].isin(['residential', 'transportation'])
  ]

  raw_data['month'] = raw_data['period'].str[:4]
  raw_data['year'] = raw_data['period'].str[-2:]

  final_columns = ['year', 'month', 'stateid', 'price', 'price-units']
  return raw_data[final_columns]


def load(dataframe: pd.DataFrame, file_path: str) -> None:
  """Load a DataFrame to a file in either CSV or Parquet format."""
  if file_path.endswith('.csv'):
    dataframe.to_csv(file_path, index=False)
  elif file_path.endswith('.parquet'):
    dataframe.to_parquet(file_path, index=False)
  else:
    raise Exception(
        f'Warning: {file_path} is not a valid file type. Please try again!'
    )


# End-to-End Orchestration
if __name__ == '__main__':
  # 1. Ingest
  raw_capability_df = extract_json_data('electricity_capability_nested.json')
  raw_sales_df = extract_tabular_data('electricity_sales.csv')

  # 2. Transform
  cleaned_sales_df = transform_electricity_sales_data(raw_sales_df)

  # 3. Load
  load(raw_capability_df, 'loaded__electricity_capability.parquet')
  load(cleaned_sales_df, 'loaded__electricity_sales.csv')
```
---
*© 2026 Ryan Tang.*
