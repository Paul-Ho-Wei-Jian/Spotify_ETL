# ETL data pipeline with Spotify API, Airflow and Snowflake

This ETL data pipeline retrieves recently played songs from Spotify’s Web API by sending a GET request for the last 48 hours of playback data. After performing data validation checks (e.g., primary key and null value checks), the data is loaded into a Snowflake database.

To streamline local development, deployment, and monitoring, I used the Astronomer library to manage multiple Apache Airflow services (Scheduler, Webserver, Worker, etc.) within a containerized environment. Airflow schedules an automatic run daily at 8:30 AM.

The python code running the project can be found under DAGs → spotify_dag.py.

## Project Screen Shot(s)
![Data_Flow](https://github.com/Paul-Ho-Wei-Jian/Spotify_ETL/blob/master/ETL_Flow.png)
![SpotifyAPIResponse](https://github.com/Paul-Ho-Wei-Jian/Spotify_ETL/blob/master/SpotifyAPIResponse.png)
![Airflow_DAG](https://github.com/Paul-Ho-Wei-Jian/Spotify_ETL/blob/master/AirflowDag.png)
![Snowflake](https://github.com/Paul-Ho-Wei-Jian/Spotify_ETL/blob/master/snowflake.png)
---
