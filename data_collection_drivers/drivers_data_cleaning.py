import pandas as pd
import data_collection_utils.data_cleaning as dc
import data_collection_utils.filling_missing_crime_data as fc
from db_utils import database_operations


def data_preprocessing_from_raw(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    It runs all functions from the data_cleaning.py file. These are doing the following:
        1. Cleaning unwanted values.
        2. Transforming to desired datatypes
        3. Creating additional columns based on the data in the dataframe
        4. Reassigning to desired tables in standard_of_living schema
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
    """
    # Cleaning unwanted values
    places_df["school_rating"] = places_df["school_rating"].map(
        dc.remove_special_character_school_rating
    )
    places_df["school_rating"] = places_df["school_rating"].str.strip()
    places_df["nightlife_rating"] = places_df["nightlife_rating"].map(
        dc.remove_special_character_nightlife_rating
    )
    places_df["nightlife_rating"] = places_df["nightlife_rating"].str.strip()
    places_df["families_rating"] = places_df["families_rating"].map(
        dc.remove_special_character_families_rating
    )
    places_df["families_rating"] = places_df["families_rating"].str.strip()
    places_df = dc.replace_incorrect_type_of_place(places_df)
    # Transforming to desired datatypes
    places_df["population"] = places_df["population"].map(dc.number_to_int)
    places_df["median_home_value"] = places_df["median_home_value"].map(
        dc.number_to_int
    )
    places_df["median_rent"] = places_df["median_rent"].map(dc.number_to_int)
    places_df["median_household_income"] = places_df["median_household_income"].map(
        dc.number_to_int
    )
    places_df = dc.create_rent_to_sell_value_ratio(places_df)
    places_df = dc.fill_missing_rent_and_home_values(places_df)
    places_df.drop("rent_sell_value_ratio", axis=1, inplace=True)
    # Creating additional columns
    places_df["state"] = places_df["link"].map(dc.create_state_from_link)
    places_df["state"] = places_df["state"].map(dc.change_state_abbreviation_to_name)
    places_df.drop(places_df[places_df["state"] == ""].index, inplace=True)
    places_df["name_with_state"] = places_df["link"].map(dc.add_name_with_state)

    return places_df


def fill_remaining_crime_data():
    """
    Reads table from the database and assigns to the DataFrame.
    Fills missing values for crime after attempt by Areavibes was taken to fill missing values after previously
    scraping Niche.com
    Saves back to the Database
    """
    crimes = database_operations.load_database_to_dataframe("crimes")
    # In case any values are "no data" (they shouldn't be)
    try:
        crimes = fc.convert_crime_values_to_numeric(crimes)
    except (ValueError, AttributeError):
        pass

    crimes = crimes.apply(fc.add_violent_crimes_ratio)
    crimes = crimes.apply(fc.add_non_violent_crimes_ratio)
    crimes = crimes.apply(fc.fill_nan_values_violent_crimes)
    crimes = crimes.apply(fc.fill_nan_values_non_violent_crimes)

    database_operations.save_dataframe_to_database(crimes, "crimes")
