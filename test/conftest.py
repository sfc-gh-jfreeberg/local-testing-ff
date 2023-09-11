import pytest
from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.connection import MockServerConnection
from project.local import get_env_var_config

def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="live")


@pytest.fixture(scope='module')
def session(request) -> Session:
    if request.config.getoption('--snowflake-session') == 'local':
        return Session(MockServerConnection())
    else:
        return Session.builder.configs(get_env_var_config()).create()
