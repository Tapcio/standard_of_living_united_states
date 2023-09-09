import pytest
import pandas as pd
import uuid
from sqlalchemy.orm import Session

from db_utils.database_connection import (
    connect_to_db,
    disconnect_from_db,
    create_sqlalchemy_session,
    create_engine,
)
from db_utils.database_operations import (
    load_database_to_dataframe,
    create_db_data_from_website_responses,
    get_object_variables_from_places_db,
    get_object_variables_from_activities_db,
    get_object_variables_from_area_feel_db,
    get_object_variables_from_crimes_db,
    get_object_variables_from_families_db,
    get_object_variables_from_wealth_db,
    get_object_variables_from_weather_db,
)


@pytest.fixture(scope="module")
def database_engine():
    engine = connect_to_db()
    yield engine  # Provide the engine to the test
    engine.dispose()  # Clean up the engine after the test


def test_connect_to_db(database_engine):
    assert database_engine is not None


def test_disconnect_from_db(database_engine):
    disconnect_from_db(database_engine)


def test_create_sqlalchemy_session(database_engine):
    session = create_sqlalchemy_session()
    assert isinstance(session, Session)
    session.close()


def test_save_dataframe_to_database(database_engine):
    test_table_name = "Families_" + str(uuid.uuid4().hex)
    test_data = {
        "unique_name": ["miasto", "city", "place"],
        "school_rating": ["A+", "A", "B+"],
        "families_rating": ["B+", "A-", "A"],
    }
    expected_df = pd.DataFrame(test_data)
    expected_df.to_sql(
        test_table_name, con=database_engine, index=False, if_exists="replace"
    )


def test_load_database_to_dataframe(database_engine):
    test_table_name = "Families_" + str(uuid.uuid4().hex)
    test_data = {
        "unique_name": ["miasto", "city", "place"],
        "school_rating": ["A+", "A", "B+"],
        "families_rating": ["B+", "A-", "A"],
    }
    expected_df = pd.DataFrame(test_data)
    expected_df.to_sql(
        test_table_name, con=database_engine, index=False, if_exists="replace"
    )
    result_df = load_database_to_dataframe(test_table_name)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_create_db_data_from_website_responses_default():
    result = create_db_data_from_website_responses(
        state="Iowa", median_household_income=80000, area_feel=["Suburban", "Urban"]
    )


def test_create_db_data_from_website_responses_with_filters():
    result = create_db_data_from_website_responses(
        state="Texas",
        median_household_income=100000,
        area_feel=["Suburban", "Rural"],
        restaurants=True,
        cafes=True,
        bars=True,
        schools=True,
        is_woman=True,
    )


def test_create_db_data_from_website_responses_no_results():
    result = create_db_data_from_website_responses(
        state="New York", median_household_income=10000, area_feel=["Suburban", "Urban"]
    )


def test_get_object_variables_from_places_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_places_db(place_name)
        assert result == (
            "Ardmore",
            "Suburb of Philadelphia, PA",
            "Pennsylvania",
            "https://www.niche.com/places-to-live/ardmore-montgomery-pa/",
        )


def test_get_object_variables_from_activities_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_activities_db(place_name)
        assert result == (
            "A",
            60,
            12,
            25,
        )


def test_get_object_variables_from_area_feel_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_area_feel_db(place_name)
        assert result == (
            "Urban Suburban Mix",
            14391,
            12,
            10,
            6,
            15,
            18,
            11,
            10,
            19,
        )


def test_get_object_variables_from_crimes_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_crimes_db(place_name)
        assert result == (
            175.274,
            3.782,
            25.234,
            84.01,
            340.068,
            1389.104,
            193.12,
        )


def test_get_object_variables_from_families_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_families_db(place_name)
        assert result == ("A+", "A+")


def test_get_object_variables_from_wealth_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_wealth_db(place_name)
        assert result == (353900, 1462, 107087)


def test_get_object_variables_from_weather_db():
    with create_sqlalchemy_session():
        place_name = "ardmore-montgomery-pa"
        result = get_object_variables_from_weather_db(place_name)
        assert result == (
            3.2243083003952573,
            17.769565217391303,
            23.671014492753624,
            8.855072463768115,
            82.48550724637681,
            104.1144927536232,
            115.8159420289855,
            93.32318840579711,
        )
