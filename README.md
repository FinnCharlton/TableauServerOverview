## Tableau Server Overview

This project was designed as an end-to-end data pipeline in Python, dbt/Snowflake and Tableau, orchestrated by Apache Airflow.

**Input:**   Tableau Server REST API\
**Output:**   Network Graph visualisation showing the network of workbooks and published datasources on a Tableau Server\
**Automation:**   Automatic daily refresh via Apache Airflow in a Docker container



![image](https://github.com/user-attachments/assets/70957355-88f1-4667-a68a-6607be3bb011)
*The network graph visualisation resulting from this data pipeline*

The pipeline is split into four processes: *ingestion, modelling, visualisation and orchestration:*

**Ingestion:** Python is used to pull relevant data from the Tableau Server REST API, tabularise the data and materialise it in Snowflake.\
**Modelling:** dbt is used to stage and model the source tables within Snowflake. A Snowflake UDTF is used to calculate coordinates for the network graph.\
**Visualisation:** Tableau is used to connect to the data model in Snowflake and visualise the network graph.\
**Orchestration:** The scripts are containerised with Docker and orchestrated to run in order using Apache Airflow.



![image](https://github.com/user-attachments/assets/9a848725-f8ec-4576-91c7-a8eda1dfcf95)
*The full data pipeline*
