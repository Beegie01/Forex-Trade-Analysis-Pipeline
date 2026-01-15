# Forex Trade Analysis Pipeline

A data engineering and analytics project designed to clean, transform, and model forex trading data for performance insights.

### Data Cleaning & Preparation Scripts
**Python**
- **[data_prep.py](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/data_prep.py)** – Cleans and standardises raw forex datasets into a consistent format for staging in the data warehouse.  
- **[push_cleansed_dim_data.py](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/push_cleansed_dim_data.py)** – Loads cleansed dimension tables into the SQL data warehouse.  
- **[push_cleansed_fact_data.py](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/push_cleansed_fact_data.py)** – Loads cleansed fact tables into the SQL data warehouse.  

### Data Transformation Scripts
**SQL**
- **[staging_ddl.sql](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/staging_ddl.sql)** – Defines schemas and creates staging and transformation tables.  
- **[core_dml.sql](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/core_dml.sql)** – Implements transformation logic and builds core data models.  
- **[access.sql](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/access.sql)** – Creates analytic-ready views for reporting, KPI calculation, and performance analysis.

### Data Loading Scripts
**Python**
-**[pull_transformed_dw_data.py](https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/pull_transformed_dw_data.py)** – Extracts analytic-ready data from data warehouse views and stores it in flat files.
