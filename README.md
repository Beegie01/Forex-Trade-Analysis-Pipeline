# Forex-Trade-Analysis

#### Data Cleaning & Preparation Scripts:
Python: 
"data_prep.py": clean and convert source datasets into a consistent format for staging in the data warehouse</br> 
"push_cleansed_dim_data.py": load cleansed dimensions into the SQL data warehouse</br> 
"push_cleansed_fact_data.py": load cleansed facts into the SQL data warehouse</br> 

#### Data Transformation Scripts:
SQL: 
"staging_ddl.sql": create schemas for staging, transformatons and staging tables</br> 
"core_dml.sql": create transformation objects</br> 
"access.sql": create final analytic-ready transformations</br> 
