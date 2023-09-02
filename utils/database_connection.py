import pandas as pd
from sqlalchemy import create_engine

from utils import utils_generic

data = utils_generic.get_config(
    loc=r"C:\Users\adamz\Documents\standard_of_living\config.yml"
)


MYSQL_HOST = data["MYSQL_HOST"]
MYSQL_USER = data["MYSQL_USER"]
MYSQL_PASSWORD = data["MYSQL_PASSWORD"]
MYSQL_DATABASE = data["MYSQL_DATABASE"]


def connect_to_db():
    """
    Establishes a database connection and returns the engine.
    """
    mysql_host = MYSQL_HOST
    mysql_user = MYSQL_USER
    mysql_password = MYSQL_PASSWORD
    mysql_database = MYSQL_DATABASE

    engine = create_engine(
        f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}",
        echo=True,  # Set echo to True to see SQL queries being executed
    )

    return engine


def disconnect_from_db(engine):
    """
    Closes the database connection.
    """
    engine.dispose()
