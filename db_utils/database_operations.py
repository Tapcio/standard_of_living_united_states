import pandas as pd

from database_connection import connect_to_db, disconnect_from_db

from data_collection_config import (
    CRIMES_COLUMNS,
    ACTIVITIES_COLUMNS,
    AREA_FEEL_COLUMNS,
    WEALTH_COLUMNS,
    FAMILIES_COLUMNS,
    PLACES_COLUMNS,
)


def load_database_to_dataframe(table_name: str) -> pd.DataFrame:
    """
    Returns data from the database in the form of DataFrame.
    Attributes:
        table_name: str
    """

    engine = connect_to_db()

    query = f"SELECT * FROM {table_name}"

    places_df = pd.read_sql(query, con=engine)

    disconnect_from_db(engine)

    return places_df


def save_dataframe_to_database(places_df: pd.DataFrame, table_name: str):
    """
    Saves the whole dataframe to the database.
    Attributes:
        table_name: str
        places_df: pd.DataFrame
    """
    engine = connect_to_db()

    places_df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    disconnect_from_db(engine)


def load_data_and_merge():
    """
    Merging all tables into one dataframe.
    """
    places = load_database_to_dataframe("places")
    weather = load_database_to_dataframe("weather")
    crimes = load_database_to_dataframe("crimes")
    activities = load_database_to_dataframe("activities")
    area_feel = load_database_to_dataframe("area_feel")
    wealth = load_database_to_dataframe("wealth")
    families = load_database_to_dataframe("families")

    dataframes = [places, area_feel, wealth, crimes, activities, families, weather]

    places_df = dataframes[0]
    for df in dataframes[1:]:
        places_df = places_df.merge(df, on="unique_name", how="outer")

    return places_df


def reassign_values_to_separate_db_tables() -> bool:
    """
    Reads the places_raw table from the database and creates separate dataframes from it. Then saves them into the
    database as separate tables: - crimes - activities - area_feel - wealth - families - places Returns: True or False
    """
    places_df = load_database_to_dataframe("places_raw")
    db_map = (
        (places_df[CRIMES_COLUMNS], "crimes"),
        (places_df[ACTIVITIES_COLUMNS], "activities"),
        (places_df[AREA_FEEL_COLUMNS], "area_feel"),
        (places_df[WEALTH_COLUMNS], "wealth"),
        (places_df[FAMILIES_COLUMNS], "families"),
        (places_df[PLACES_COLUMNS], "places"),
    )

    db_sanity_check = map(lambda x: save_dataframe_to_database(*x), db_map)

    if all(db_sanity_check):
        return True
    else:
        return False
