import pandas as pd
from utils import weather
from utils import preprocessing
from utils import database_connection

import utils.data_cleaning as dc

from drivers_data_collection import (
    scrape_all_places_and_links_niche,
    scrape_all_info_from_place_niche,
    scrape_areavibes_and_fill_missing_data,
    google_data_getter,
)

from drivers_data_cleaning import (
    data_preprocessing_from_raw,
    fill_remaining_crime_data,
)


def get_all_data():
    """
    Driver to enable to get all data by one click and save to csv.
    """
    scrape_all_places_and_links_niche(1, 2000)
    scrape_all_info_from_place_niche()
    places_df = pd.read_csv("niche_all_scraped_data_raw.csv")
    places_df = scrape_areavibes_and_fill_missing_data(places_df)
    places_df = google_data_getter(places_df)
    places_df = weather.create_temperature_df(places_df)
    places_df = weather.fill_missing_weather_values(places_df)
    places_df.to_csv("niche_all_scraped_data_raw.csv", index=False)


def clean_all_data():
    """
    Driver to clean all data and reassign to appropriate tables.
    """
    places_df = pd.read_csv("data_preprocessing_from_raw.csv")
    places_df = data_preprocessing_from_raw(places_df)
    database_connection.save_dataframe_to_database(places_df, "places_raw")
    fill_remaining_crime_data()
    places_df = preprocessing.load_data_and_merge()
    places_df = preprocessing.fill_missing_school_ratings(places_df)
    places_df = preprocessing.drop_places_with_missing_weather_data(places_df)
    places_df = places_df.dropna().reset_index()
    database_connection.save_dataframe_to_database(places_df, "places_raw")
    dc.reassign_values_to_separate_db_tables()
