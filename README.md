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

## Ingestion

The ingestion process comprises three steps: API calls, tabularisation and materialisation in Snowflake.\
The API calls and tabularisation is handled by the tableauServer class in tsc.py, and utilises the *tableauserverclient module* to interface with the REST API, alongside the *pandas* module to tabularise the resulting data in a DataFrame:

```python
class tableauServer:
    def __init__(self,url,site,pat,patSecret):
        self.url = url
        self.site = site
        self.pat = pat
        self.patSecret = patSecret
        self.tokenAuth = TSC.PersonalAccessTokenAuth(self.pat,self.patSecret,self.site)
        self.server = TSC.Server(self.url,use_server_version=True)

    def auth(self):
        try:
            self.server.auth.sign_in(self.tokenAuth)
            print("Signin Successful")
        except Exception as e:
            print(e)
```
*Definition of class initialisation and Tableau Server authorisation functions*


```python
    def dfParse(self, objList):
        df = pd.DataFrame([vars(obj) for obj in objList])
        df["site"] = self.site
        return df

    def get_workbooks(self):
        with self.server.auth.sign_in(self.tokenAuth):
            results = [wb for wb in TSC.Pager(self.server.workbooks)]
            return self.dfParse(results)
```
*Example of API call and DataFrame tabularisation functions*

Materialisation in Snowflake is handled by the SnowflakeConnector class in snowflake_connector.py, leveraging the *snowflake* module and especially the *write_pandas()* method:

```python
    def write(self,df,dfName):
        try:
            write_pandas(self.conn,df,dfName,auto_create_table=True,use_logical_type=True,overwrite=True)
            print("Table written successfully")
        except Exception as e:
            print(f"Error: {e}")

    def ingest(self,upload_info):
        self.auth()
        for element in upload_info:
            name = element["name"]
            content = element["content"]
            try:
                self.write(content,name)
                print(f"Uploaded {name}")
            except Exception as e:
                print(f"Error uploading {name} : {e}")
        try:
            self.conn.close()
            print("Connection Closed")
        except Exception as e:
            print(f"Error Closing Connection: {e}")
```
*Defintion of the 'write' function to materialise a single dataframe in Snowflake, and the 'ingest' function to materialise multiple dataframes from a name:content dictionary*

## Modelling

Modelling is handled by dbt, and was designed to provide a data mart to analyse datasource-workbook connections, as well as user and usage information. It also produces auxilliary tables used to create the network graph visualisation. This visualisation requires the application of a force-directed graphing algorithm to the datasource-workbook connections table, which was achieved using a user-defined table function (UDTF) in Snowflake:

![image](https://github.com/user-attachments/assets/4ab9c0ba-0fd3-42eb-b76e-b062aa658047)
*Full dbt modelling DAG*

```sql
CREATE OR REPLACE FUNCTION TIL_PORTFOLIO_PROJECTS.TABLEAUSERVER_OVERVIEW.NETWORK_GRAPH("WORKBOOK_ID" VARCHAR(16777216), "DATASOURCE_NAME" VARCHAR(16777216), "SITE" VARCHAR(16777216))
RETURNS TABLE ("NODE" VARCHAR(16777216), "X" FLOAT, "Y" FLOAT, "TS_SITE" VARCHAR(16777216))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('pandas','networkx','scipy')
HANDLER = 'handler'
AS '
from _snowflake import vectorized
import pandas
import networkx
import scipy

class handler:

    @vectorized(input=pandas.DataFrame)
    def end_partition(self, df):

            df.columns = ["workbook_id","datasource_name","site"]
            
            # Initialise the graph
            G = networkx.Graph()

            #Initialise row counting variable
            cnt = 0

            # Add edges to the graph
            for _, row in df.iterrows():
                G.add_edge(row[0], row[1])
                cnt += 1

            #Calculate k variable for spring layout algorithm
            k_value = (1/(cnt**0.5)) * 1.15

            # Compute the spring layout
            force_directed_graph = networkx.spring_layout(G, k=k_value)

            # Convert the layout dictionary to pandas DataFrame
            df_graph = pandas.DataFrame.from_dict(force_directed_graph, orient=''index'').reset_index()
            df_graph.columns = [''node'', ''x'', ''y'']
            df_graph["site"] = df.at[1, "site"]

            return df_graph
';
```
*Snowflake UDTF to apply a force-directed graphing algorithm to the datasource-workbook connections table*

## Visualisation

The network graph visualtion was built in Tableau Desktop and is hosted in Tableau Server, connecting live to the Snowflake data mart. Some exploratory functionality was built into the visualisation, including the ability to choose which site to view and the ability to search for users and content names, which will highlight them on the graph:

![image](https://github.com/user-attachments/assets/1f0f1a70-f082-4391-8790-31a0ebbb36eb)
*Demonstration of visualisation functionality*

## Orchestration

To productionise, the codebase is cloned in a Docker container (not included in this repo). This container runs Apache Airflow to orchestrate the ingestion and modelling steps on a daily schedule:

```python
with DAG(

    #Define DAG name and run schedule
    dag_id='tableauserver_overview',
    start_date=datetime.datetime(2024, 8, 19),
    schedule_interval='@daily',
    catchup=True,
    tags=['prod','python','dbt'],

) as dag:
    
    #Task 1 runs the ingestion script
    task_1 = BashOperator(
    task_id='ingestion_task',
    bash_command="python /opt/airflow/dags/src/main.py"
    )

    #Task 2 navigates to the dbt project folder and runs the build command, specifying the location of profiles.yml
    task_2 = BashOperator(
    task_id='modelling_task',
    bash_command="cd /opt/airflow/plugins/tableauserver_overview && dbt build --profiles-dir ..",
    )

    #Task 2 specified as downstream of Task 1
    task_1 >> task_2
```
*Definition of the Airflow DAG*

![image](https://github.com/user-attachments/assets/47d6524e-3313-43b0-849a-876ae49b9db8)\
*DAG in the Airflow webserver*


