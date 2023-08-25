import pandas as pd
from sqlalchemy import create_engine

import utils.utils as u

data = u.get_config()

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


def load_database_to_dataframe(table_name: str) -> pd.DataFrame:
    """
    Returns data from the database in the form of DataFrame.
    Attributes:
        str: table_name
    """

    engine = connect_to_db()

    query = f"SELECT * FROM {table_name}"

    places_df = pd.read_sql(query, con=engine)

    disconnect_from_db(engine)

    return places_df


def save_dataframe_to_database(places_df: pd.DataFrame, table_name: str) -> None:
    """
    Saves the whole dataframe to the database.
    Attributes:
        str: table_name
        DataFrame: places_df
    """
    engine = connect_to_db()

    places_df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    disconnect_from_db(engine)
