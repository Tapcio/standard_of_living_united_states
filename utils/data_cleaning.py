import pandas as pd
import numpy as np
from utils import database_operations

from config import (
    US_STATES,
    CRIMES_COLUMNS,
    ACTIVITIES_COLUMNS,
    AREA_FEEL_COLUMNS,
    WEALTH_COLUMNS,
    FAMILIES_COLUMNS,
    PLACES_COLUMNS,
    US_AVERAGE_CRIMES,
    VIOLENT_CRIMES_COLUMNS,
    NON_VIOLENT_CRIMES_COLUMNS,
)


def remove_special_character_school_rating(school_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Args:
        school_rating: str
    Returns:
        school_rating: str
    """
    return school_rating.replace("Â", "").replace("\xa0", "").strip()


def remove_special_character_nightlife_rating(nightlife_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Args:
        nightlife_rating: str
    Returns:
        nightlife_rating: str
    """
    return nightlife_rating.replace("gradeÂ", "").replace("minus", "-").replace(" ", "")


def remove_special_character_families_rating(families_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Args:
        families_rating: str
    Returns:
        families_rating: str
    """
    return families_rating.replace("gradeÂ", "").replace("minus", "-").replace(" ", "")


def create_state_from_link(link: str) -> str:
    """
    Gets the state Abbreviation from the link
    Args:
        link: str
    Returns:
        state: str
    """
    split_url = link.split("/")[-2]
    return split_url[-2:].upper()


def change_state_abbreviation_to_name(abbreviation: str) -> str:
    """
    Replicates full State name using dictionary with state names
    Args:
        abbreviation: str
    Returns:
        state_name: str
    """
    return US_STATES.get(abbreviation, "")


def number_to_int(number: str) -> int | float:
    """
    Creates int from the string that contains commas or $ signs
    Args:
        number: str
    Returns:
        number: int
    """
    try:
        return int(number.replace(",", "").replace("$", ""))
    except ValueError:
        return np.nan


def replace_incorrect_type_of_place(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds rows with incorrect type_of_place values and replaces with correct.
    Args:
        places_df: pd.DataFrame

    Returns:
        places_df: pd.DataFrame
    """
    place_types_list = ["City", "Town", "Neighborhood", "Suburb"]
    for index, row in places_df.iterrows():
        split_place_type = row["type_of_place"].split(" ")
        if split_place_type[0] not in place_types_list:
            new_type = fill_missing_type_of_place(row["link"])
            places_df.loc[index, "type_of_place"] = new_type
    return places_df


def fill_missing_type_of_place(link: str) -> str | float:
    """
    Link has "/n/" part for Neighbourhoods, otherwise it will be Suburb.
    Towns or Cities are captured correctly when scraping, so only creating
    Neighbourhood or Suburb.
    Args:
        link: str
    Returns:
        type_of_place: str
    """
    link_split_list = link.split("/")
    if link_split_list[4] == "n":
        place_split = link_split_list[5].split("-")
        return (
            f"Neighborhood in {place_split[-2].capitalize()}, {place_split[-1].upper()}"
        )
    elif link_split_list[4] != "n":
        place_split = link_split_list[4].split("-")
        return f"Suburb of {place_split[-2].capitalize()}, {place_split[-1].upper()}"
    else:
        return np.nan


def remove_niche_prefix(link: str) -> str:
    """
    Removes the generic part of the link and returns only the dynamic one that is related to the place
    Args:
        link: str
    Returns:
        dynamic_link_part : str
    """
    prefix1 = "https://www.niche.com/places-to-live/"
    prefix2 = "https://www.niche.com/places-to-live/n/"

    if link.startswith(prefix2):
        return link[len(prefix2) :][:-1]
    else:
        return link[len(prefix1) :][:-1]


def add_name_with_state(link: str) -> str:
    """
    Returns name place based on the length of the link's dynamic part.
    Args:
        link: str
    Returns:
        name_with_state: str
    """

    link_without_prefix = remove_niche_prefix(link)
    place_name_split = link_without_prefix.split("-")

    if len(place_name_split) == 5:
        place = f"{place_name_split[0].capitalize()} {place_name_split[1].capitalize()}"
        city_or_county = (
            f"{place_name_split[2].capitalize()} {place_name_split[3].capitalize()}"
        )
        state = place_name_split[4].upper()
    elif len(place_name_split) == 4:
        place = f"{place_name_split[0].capitalize()} {place_name_split[1].capitalize()}"
        city_or_county = place_name_split[2].capitalize()
        state = place_name_split[3].upper()
    elif len(place_name_split) == 3:
        place = place_name_split[0].capitalize()
        city_or_county = place_name_split[1].capitalize()
        state = place_name_split[2].upper()
    else:
        return f"{place_name_split[0].capitalize()}, {place_name_split[1].upper()}"
    return f"{place}, {city_or_county}, {state}"


def create_rent_to_sell_value_ratio(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates column with rent to sell ratio
    Args:
        places_df: pd.DataFrame

    Returns:
        places_df: pd.DataFrame
    """
    places_df["median_home_value"] = pd.to_numeric(
        places_df["median_home_value"], errors="coerce"
    )
    places_df["median_rent"] = pd.to_numeric(places_df["median_rent"], errors="coerce")
    places_df["rent_sell_value_ratio"] = (
        places_df["median_rent"] / places_df["median_home_value"]
    )

    return places_df


def fill_missing_rent_and_home_values(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    If any of the values for median_home_value or median_rent are NaN, the function uses the rent_to_sell ratio to
    assign missing values.
    Args:
        places_df: pd.DataFrame

    Returns:
        places_df: pd.DataFrame
    """
    rent_sell_value_ratio = places_df.rent_sell_value_ratio.mean()
    for index, row in places_df.iterrows():
        if pd.isna(row["median_home_value"]):
            try:
                places_df.loc[index, "median_home_value"] = round(
                    row["median_rent"] * rent_sell_value_ratio, 2
                )
            except (ValueError, KeyError):
                pass
        elif pd.isna(row["median_rent"]):
            try:
                places_df.loc[index, "median_rent"] = round(
                    row["median_home_value"] * rent_sell_value_ratio, 2
                )
            except (ValueError, KeyError):
                pass

    return places_df


def convert_crime_values_to_numeric(crimes: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms into floats and "no data" into NaN, so it is easier to fill missing values later.
    Args:
        crimes: pd.DataFrame
    Returns:
        crimes: pd.DataFrame
    """
    numeric_columns = crimes.columns.difference(["unique_name"])
    crimes[numeric_columns] = crimes[numeric_columns].applymap(
        lambda x: float(x) if x != "no data" else np.nan
    )

    return crimes


def add_violent_crimes_ratio(row: pd.Series) -> float:
    """
    Calculates Ratio of violent crimes based on values present in the dataframe.
    Args:
        row: pd.Series

    Returns:
        ratio: float
    """
    non_nan_counter = 0
    crime_ratio_sum = 0

    for column in VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        if np.isnan(crime_value):
            pass
        elif crime_value == 0:
            non_nan_counter += 1
        else:
            crime_ratio_sum += (
                crime_value / US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
            )
            non_nan_counter += 1

    if non_nan_counter == 0:
        non_nan_counter = 1
    else:
        pass

    return crime_ratio_sum / non_nan_counter


def add_non_violent_crimes_ratio(row: pd.Series) -> float:
    """
    Calculates Ratio of non-violent crimes based on values present in the dataframe.
    Args:
        row: pd.Series

    Returns:
        ratio: float
    """
    non_nan_counter = 0
    crime_ratio_sum = 0

    for column in NON_VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        if np.isnan(crime_value):
            pass
        elif crime_value == 0:
            non_nan_counter += 1
        else:
            crime_ratio_sum += (
                crime_value / US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
            )
            non_nan_counter += 1

    if non_nan_counter == 0:
        non_nan_counter = 1
    else:
        pass

    return crime_ratio_sum / non_nan_counter


def fill_nan_values_violent_crimes(row: pd.Series) -> pd.Series:
    """
    Fills NaN values for violent crimes based on the violent crimes ratio
    Args:
        row: pd.Series

    Returns:
        row: pd.Series
    """
    for column in VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]

        if np.isnan(crime_value):
            row[column] = (
                row["violent_crime_ratio"]
                * US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
            )
        else:
            pass

    return row


def fill_nan_values_non_violent_crimes(row: pd.Series) -> pd.Series:
    """
    Fills NaN values for non-violent crimes based on the non-violent crimes ratio
    Args:
        row: pd.Series

    Returns:
        row: pd.Series
    """
    for column in NON_VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]

        if np.isnan(crime_value):
            row[column] = (
                row["non_violent_crime_ratio"]
                * US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
            )
        else:
            pass

    return row


def reassign_values_to_separate_db_tables() -> bool:
    """
    Reads the places_raw table from the database and creates separate dataframes from it. Then saves them into the
    database as separate tables: - crimes - activities - area_feel - wealth - families - places Returns: True or False
    """
    places_df = database_operations.load_database_to_dataframe("places_raw")
    db_map = (
        (places_df[CRIMES_COLUMNS], "crimes"),
        (places_df[ACTIVITIES_COLUMNS], "activities"),
        (places_df[AREA_FEEL_COLUMNS], "area_feel"),
        (places_df[WEALTH_COLUMNS], "wealth"),
        (places_df[FAMILIES_COLUMNS], "families"),
        (places_df[PLACES_COLUMNS], "places"),
    )

    db_sanity_check = map(
        lambda x: database_operations.save_dataframe_to_database(*x), db_map
    )

    if all(db_sanity_check):
        return True
    else:
        return False
