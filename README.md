# Forex Trade Analysis Pipeline

A data engineering and analytics project designed to clean, transform, and model forex trading data for performance insights.

### Data Cleaning & Preparation Scripts
**Python**
- **data_prep.py** – Cleans and standardises raw forex datasets into a consistent format for staging in the data warehouse.  
- **push_cleansed_dim_data.py** – Loads cleansed dimension tables into the SQL data warehouse.  
- **push_cleansed_fact_data.py** – Loads cleansed fact tables into the SQL data warehouse.  

### Data Transformation Scripts
**SQL**
- **(https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/staging_ddl.sql)[staging_ddl.sql]** – Defines schemas and creates staging and transformation tables.  
- **https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/core_dml.sql core_dml.sql** – Implements transformation logic and builds core data models.  
- **https://github.com/Beegie01/Forex-Trade-Analysis-Pipeline/blob/main/access.sql access.sql** – Creates analytic-ready views for reporting, KPI calculation, and performance analysis.  
