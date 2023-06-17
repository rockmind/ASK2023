# Application of Apache Airflow for ETL 

## Environment requirements

ubuntu (version 22.04.2)  
docker (version 20.10.24)

## Run locally

### Build image
1. Customize airflow docker image: change variables in `.env`
2. Run docker engine.
3. Open terminal from project root, run `./build_image.sh`

### Run airflow
1. Run `./run_locally.sh`
2. Airflow starts at `localhost:8080`
3. Default user/password: `ask-airflow`
4. Connection to databse:
   - host: `localhost`
   - port: `2345`
   - database: `ask_db` 
   - user/password: `ask_postgres_user`
   
For more information see:
[Running Apache Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Run dags
1. Firstly configure Airflow Connection
   1. Google Cloud:
      - Connection Id: `ask_google_cloud`
      - Connection Type: Google Cloud
      - Scopes: `https://www.googleapis.com/auth/spreadsheets`
      - Project Id: `{from your Google Cloud}`
      - Keyfile Path/Keyfile JSON: `{from your Google Cloud}` 
   2. Postgres(default)
      - Connection Id: `ask_db`
      - Connection Type: Postgres
      - Host: `ask_postgres_db`
      - Schema: `ask_db`
      - Login/Password: `ask_postgres_user`
2. Add Airflow Variables
   1. Google Sheets
      - Key: `ask_google_sheet_id`
      - Value: `{google_sheet_id}` from spreadsheet URL: `https://docs.google.com/spreadsheets/d/{google_sheet_id}/`
   2. Database
      - Key: `ask_db_schema_name`
      - Value: `synchronizing_sheets` (default)
3. Make sure that your Google Cloud service account has read access  
to Google spreadsheet given in Airflow variable `ask_google_sheet_id` 

## Development
Dependencies for local development can be found in `requirements.txt` file.
Install by 

    pip install -r requirements.txt

Using virtual envinment (eg. venv) is recommended.

If following error occurs

> <font color="red">ERROR: Could not build wheels for pendulum, which is required to install pyproject.toml-based projects</font>

install packets:

    sudo apt-get install build-essential libssl-dev libffi-dev python3-dev