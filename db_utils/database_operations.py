import pandas as pd
from sqlalchemy.orm import sessionmaker

from db_utils.database_connection import connect_to_db, disconnect_from_db

from data_collection_config import (
    CRIMES_COLUMNS,
    ACTIVITIES_COLUMNS,
    AREA_FEEL_COLUMNS,
    WEALTH_COLUMNS,
    FAMILIES_COLUMNS,
    PLACES_COLUMNS,
)

from data_collection_config import (
    SELECT_PART,
    FAMILIES_QUERY,
    SCHOOLS_QUERY,
    NIGHTLIFE_QUERY,
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


def send_query_to_db(query: str) -> any:
    """
    Sends a query to the database and receives a result back.
    Args:
        query: str

    Returns:
        query_results: any
    """
    engine = connect_to_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    query_results = session.execute(query)

    return query_results


def get_responses():
    """

    Returns:

    """
    state = "Colorado"
    median_household_income = 120000
    area_feel = ["Suburban", "Dense Suburban", "Urban"]
    is_woman = False
    nightlife = False
    families = True
    schools = True
    restaurants = True
    bars = False
    cafes = True

    return create_db_data_from_website_responses(
        state,
        median_household_income,
        area_feel,
        is_woman,
        nightlife,
        families,
        schools,
        restaurants,
        bars,
        cafes,
    )


def create_db_data_from_website_responses(
    state: str,
    median_household_income: int,
    area_feel: list,
    is_woman: bool = False,
    nightlife: bool = False,
    families: bool = False,
    schools: bool = False,
    restaurants: bool = False,
    bars: bool = False,
    cafes: bool = False,
) -> list:
    """
    Creating SQL query based on the user responses.
    Args:
        state: str
        median_household_income: int
        area_feel: list
        is_woman: bool
        nightlife: bool
        families: bool
        schools: bool
        restaurants: bool
        bars: bool
        cafes: bool

    Returns:
        list_of_places: list
    """
    state_query = f"WHERE \np.state = '{state}'"
    median_household_income_query = (
        f"\nAND w.median_household_income < {median_household_income}"
    )
    if restaurants:
        restaurants_query = f"\nAND a.restaurants > 20"
    else:
        restaurants_query = ""

    if cafes:
        cafes_query = f"\nAND a.cafes > 10"
    else:
        cafes_query = ""

    if bars:
        bars_query = f"\nAND a.bars > 10"
    else:
        bars_query = ""
    area_feel_str = ", ".join(f"'{area_type}'" for area_type in area_feel)
    area_feel_query = f"\nAND af.area_feel IN ({area_feel_str})"
    order_by_query = "\nORDER BY"
    if families:
        families_query = FAMILIES_QUERY
    else:
        families_query = ""

    if schools:
        schools_query = SCHOOLS_QUERY
    else:
        schools_query = ""

    if nightlife:
        nightlife_query = NIGHTLIFE_QUERY
    else:
        nightlife_query = ""

    if is_woman:
        sorting_query = "\nc.Rape, c.Assault, c.Murder, c.Robbery, c.Theft DESC"
    else:
        sorting_query = "\nc.Assault, c.Murder, c.Rape, c.Robbery, c.Theft DESC"

    limit_query = "\n LIMIT 10;"
    query = (
        SELECT_PART
        + state_query
        + median_household_income_query
        + restaurants_query
        + cafes_query
        + bars_query
        + area_feel_query
        + order_by_query
        + families_query
        + schools_query
        + nightlife_query
        + sorting_query
        + limit_query
    )

    query_results = send_query_to_db(query)
    list_of_places = []
    for place in query_results:
        list_of_places.append(place.unique_name)

    return list_of_places


def create_objects_for_places(list_of_places: list):
    """
    Creates dataclass objects for each db_data:
        Places, Activities, Area_Feel, Crimes, Families, Wealth, Weather
    Args:
        list_of_places: list

    Returns:
        dataclass_objects: objects
    """
    places_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Places"
    )
    activities_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Activities"
    )
    area_feel_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="AreaFeel"
    )
    crimes_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Crimes"
    )
    families_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Families"
    )
    wealth_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Wealth"
    )
    weather_objects = query_all_columns_to_dataclass(
        list_of_places=list_of_places, table_name="Weather"
    )

    return (
        places_objects,
        activities_objects,
        area_feel_objects,
        crimes_objects,
        families_objects,
        wealth_objects,
        weather_objects,
    )


def query_all_columns_to_dataclass(list_of_places: list, table_name: str):
    """
    Returns results of a query as data_classes.Places objects.
    Args:
        list_of_places: list -> List of unique places
        table_name: str -> Name of the table in the database and sqlalchemy class
    Returns:
        dataclass_objects: objects
    """
    class_table = globals().get(table_name)
    if class_table is None:
        raise ValueError(f"Class for table '{table_name}' is not defined.")
    columns = class_table.__table__.columns
    unique_places = ", ".join(f"'{place}'" for place in list_of_places)

    query = f"SELECT * FROM {table_name.lower()} WHERE unique_name IN ({unique_places})"

    engine = connect_to_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    query_results = session.execute(query)

    dataclass_objects = [
        class_table(**{column.key: getattr(row, column.key) for column in columns})
        for row in query_results
    ]

    return dataclass_objects


get_responses()
