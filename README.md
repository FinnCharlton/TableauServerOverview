## Tableau Server Overview

This project was designed as an end-to-end data pipeline in Python, dbt/Snowflake and Tableau, orchestrated by Apache Airflow.

**Input:**   Tableau Server REST API\
**Output:**   Network Graph visualisation showing the network of workbooks and published datasources on a Tableau Server\
**Automation:**   Automatic daily refresh via Apache Airflow in a Docker container



![image](https://github.com/user-attachments/assets/70957355-88f1-4667-a68a-6607be3bb011)
*The network graph visualisation resulting from this data pipeline*

The pipeline is split into four processes: *ingestion, modelling, visualisation and orchestration:*

**Ingestion**: 
