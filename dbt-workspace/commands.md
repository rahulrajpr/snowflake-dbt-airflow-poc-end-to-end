dbt debug
dbt compile --select model_name
dbt run --full-refresh --select model_name
dbt deps --profiles-dir .
dbt list
dbt list --resource-type model