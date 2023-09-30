"""
database_operations contain:
    - loading database to the dataframe
    - saving dataframe to the database
    - merging dataframes and reassigning data to appropriate tables
    - sending queries to the database based on the specific input
    - loading data from SQLAlchemy dataclasses
"""
import pandas as pd
from toolz import pipe

from db_utils.database_connection import (
    connect_to_db,
    disconnect_from_db,
    create_sqlalchemy_session,
)

from config import (
    CRIMES_COLUMNS,
    ACTIVITIES_COLUMNS,
    AREA_FEEL_COLUMNS,
    WEALTH_COLUMNS,
    FAMILIES_COLUMNS,
    PLACES_COLUMNS,
)

from config import (
    SELECT_PART,
    FAMILIES_QUERY,
    SCHOOLS_QUERY,
    NIGHTLIFE_QUERY,
)


from db_utils.data_classes import (
    Places,
    Activities,
    AreaFeel,
    Crimes,
    Families,
    Wealth,
    Weather,
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
    session = create_sqlalchemy_session()
    query_results = session.execute(query)

    return query_results


def get_responses():
    """
    Now it has dummy responses. To be replaced once the front-end is done.
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


def get_object_variables_from_places_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        name: str
        type_of_place: str
        state: str
        link: str
    """
    session = create_sqlalchemy_session()
    place = session.query(Places).filter_by(unique_name=place_name).first()

    name = place.name
    type_of_place = place.type_of_place
    state = place.state
    link = place.link

    return name, type_of_place, state, link


def get_object_variables_from_activities_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        nightlife_rating: str
        restaurants: int
        bars: int
        cafes: int
    """
    session = create_sqlalchemy_session()
    activity = session.query(Activities).filter_by(unique_name=place_name).first()

    nightlife_rating = activity.nightlife_rating
    restaurants = activity.restaurants
    bars = activity.bars
    cafes = activity.cafes

    return nightlife_rating, restaurants, bars, cafes


def get_object_variables_from_area_feel_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        area_feel: str
        population: int
        under_ten: int
        ten_to_seventeen: int
        eighteen_to_twentyfour: int
        twentyfive_to_thirtyfour: int
        thirtyfive_to_fourtyfour: int
        fourtyfive_to_fiftyfour: int
        fiftyfive_to_sixtyfour: int
        over_sixtyfive: int
    """
    session = create_sqlalchemy_session()
    area_feels = session.query(AreaFeel).filter_by(unique_name=place_name).first()

    area_feel = area_feels.area_feel
    population = area_feels.population
    under_ten = area_feels.under_ten
    ten_to_seventeen = area_feels.ten_to_seventeen
    eighteen_to_twentyfour = area_feels.eighteen_to_twentyfour
    twentyfive_to_thirtyfour = area_feels.twentyfive_to_thirtyfour
    thirtyfive_to_fourtyfour = area_feels.thirtyfive_to_fourtyfour
    fourtyfive_to_fiftyfour = area_feels.fourtyfive_to_fiftyfour
    fiftyfive_to_sixtyfour = area_feels.fiftyfive_to_sixtyfour
    over_sixtyfive = area_feels.over_sixtyfive

    return (
        area_feel,
        population,
        under_ten,
        ten_to_seventeen,
        eighteen_to_twentyfour,
        twentyfive_to_thirtyfour,
        thirtyfive_to_fourtyfour,
        fourtyfive_to_fiftyfour,
        fiftyfive_to_sixtyfour,
        over_sixtyfive,
    )


def get_object_variables_from_crimes_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        assault: float
        murder: float
        rape: float
        robbery: float
        burglary: float
        theft: float
        motor_vehicle_theft: float
    """
    session = create_sqlalchemy_session()
    crime = session.query(Crimes).filter_by(unique_name=place_name).first()

    assault = crime.assault
    murder = crime.murder
    rape = crime.rape
    robbery = crime.robbery
    burglary = crime.burglary
    theft = crime.theft
    motor_vehicle_theft = crime.motor_vehicle_theft

    return assault, murder, rape, robbery, burglary, theft, motor_vehicle_theft


def get_object_variables_from_families_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
          school_rating: str
          families_rating: str
    """
    session = create_sqlalchemy_session()
    families = session.query(Families).filter_by(unique_name=place_name).first()

    school_rating = families.school_rating
    families_rating = families.families_rating

    return school_rating, families_rating


def get_object_variables_from_wealth_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        median_home_value: int
        median_rent: int
        median_household_income: int
    """
    session = create_sqlalchemy_session()
    wealth = session.query(Wealth).filter_by(unique_name=place_name).first()

    median_home_value = wealth.median_home_value
    median_rent = wealth.median_rent
    median_household_income = wealth.median_household_income

    return median_home_value, median_rent, median_household_income


def get_object_variables_from_weather_db(place_name: str) -> tuple:
    """
    Retrieves object attributes from the database using sqlalchemy dataclasses.
    Args:
        place_name: str
    Returns:
        temp_first_quarter: float
        temp_second_quarter: float
        temp_third_quarter: float
        temp_fourth_quarter: float
        prcp_first_quarter: float
        prcp_second_quarter: float
        prcp_third_quarter: float
        prcp_fourth_quarter: float
    """
    session = create_sqlalchemy_session()
    weather = session.query(Weather).filter_by(unique_name=place_name).first()

    temp_first_quarter = weather.temp_first_quarter
    temp_second_quarter = weather.temp_second_quarter
    temp_third_quarter = weather.temp_third_quarter
    temp_fourth_quarter = weather.temp_fourth_quarter
    prcp_first_quarter = weather.prcp_first_quarter
    prcp_second_quarter = weather.prcp_second_quarter
    prcp_third_quarter = weather.prcp_third_quarter
    prcp_fourth_quarter = weather.prcp_fourth_quarter

    return (
        temp_first_quarter,
        temp_second_quarter,
        temp_third_quarter,
        temp_fourth_quarter,
        prcp_first_quarter,
        prcp_second_quarter,
        prcp_third_quarter,
        prcp_fourth_quarter,
    )


def return_places(responses):
    pipe(
        responses,
        create_db_data_from_website_responses,
        get_object_variables_from_places_db,
        get_object_variables_from_wealth_db,
        get_object_variables_from_activities_db,
        get_object_variables_from_area_feel_db,
        get_object_variables_from_crimes_db,
        get_object_variables_from_families_db,
    )
