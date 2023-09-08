# Local Testing Demo

## Setup

1. Fork this repo and clone it locally
1. Create conda env:

    ```
    conda env create -f environment.yml
    conda activate snowpark-local-test-ff
    ```

2. Set your account credentials:

    ```bash
    # Linux/MacOS
    export SNOWSQL_ACCOUNT=<replace with your account identifer>
    export SNOWSQL_USER=<replace with your username>
    export SNOWSQL_ROLE=<replace with your role>
    export SNOWSQL_PWD=<replace with your password>
    export SNOWSQL_DATABASE=<replace with your database>
    export SNOWSQL_SCHEMA=<replace with your schema>
    export SNOWSQL_WAREHOUSE=<replace with your warehouse>
    ```

## Run demo

### Run tests with local testing

```bash
pytest --snowflake-session local
```

#### Run tests with a live connnection

```bash
pytest
```

#### Showcase GitHub Actions

1. Open a pull request on **your** fork of the repo (not the upstream)
1. Go to your repository on GitHub.com and click "Actions" to see the actions execution