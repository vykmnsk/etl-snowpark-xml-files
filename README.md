# Ingest XML files into Snowflake using Snowpark and CI/CD


## Install

### Create Python Virtual Environment [Optional]
    python -m venv .venv
    source .venv/bin/activate
    which python
    which pip

### Install required python app libs
    pip install -r reqs.txt


### Run locally
    python app/procedures.py


### Install local snowpark client
    pip install -U snowflake-cli && snow --version


### Set the environment variables
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA


### (Re)create required DB objects (do NOT run in production!)
    snow sql -f sql/setup_db.sql
    snow sql -f sql/setup_tables.sql
    snow sql -f sql/setup_tables_alter1.sql


### Deploy function(s) and procedure(s) to Snowflake and execute there
    snow snowpark build
    snow snowpark deploy --replace
    snow snowpark execute procedure "unpack_xmls()"
