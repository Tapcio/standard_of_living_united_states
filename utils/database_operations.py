import yaml
import pandas as pd
from sqlalchemy import create_engine

from utils.utils import get_config

config = get_config()

MYSQL_HOST = config["MYSQL_HOST"]
MYSQL_USER = config["MYSQL_USER"]
MYSQL_PASSWORD = config["MYSQL_PASSWORD"]
MYSQL_DATABASE = config["MYSQL_DATABASE"]


def load_database_to_dataframe(table_name: str) -> pd.DataFrame:
    """
    Returns data from the database in the form of DataFrame.
    Attributes:
        str: table_name
    """
    mysql_host = MYSQL_HOST
    mysql_user = MYSQL_USER
    mysql_password = MYSQL_PASSWORD
    mysql_database = MYSQL_DATABASE

    engine = create_engine(
        f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}",
        echo=True,  # Set echo to True to see SQL queries being executed
    )

    query = f"SELECT * FROM {table_name}"

    places_df = pd.read_sql(query, con=engine)

    engine.dispose()

    return places_df


def save_dataframe_to_database(places_df: pd.DataFrame, table_name: str) -> None:
    """
    Saves the whole dataframe to the database.
    Attributes:
        str: table_name
        DataFrame: places_df
    """
    mysql_host = MYSQL_HOST
    mysql_user = MYSQL_USER
    mysql_password = MYSQL_PASSWORD
    mysql_database = MYSQL_DATABASE

    engine = create_engine(
        f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}",
        echo=True,  # Set echo to True to see SQL queries being executed
    )

    places_df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    engine.dispose()
