"""
Crimes specific file with functions, as Crime values are the most tricky in the whole dataset.
"""
import haversine as hs
import pandas as pd
import numpy as np

from config import (
    AREA_WEALTH_THRESHOLD,
    US_AVERAGE_CRIMES,
    VIOLENT_CRIMES_COLUMNS,
    NON_VIOLENT_CRIMES_COLUMNS,
)


def find_closest_places(places_df: pd.DataFrame, place_index: int) -> list:
    """
    Search for closest places in the 5km radius.
    Args:
        places_df: pd.DataFrame
        place_index: int
    Returns:
        closest_places: list -> list of closest places sorted starting from the closest place.
    """
    selected_row = places_df.loc[place_index]
    selected_state = selected_row["state"]
    selected_coordinates = (selected_row["latitude"], selected_row["longitude"])

    closest_places = []

    for index, row in places_df.iterrows():
        if index != place_index and row["state"] == selected_state:
            compared_coordinates = (row["latitude"], row["longitude"])
            distance = hs.haversine(
                selected_coordinates, compared_coordinates, unit=hs.Unit.KILOMETERS
            )
            if distance < 5:
                closest_places.append((index, distance))

    closest_places.sort(key=lambda x: x[1])

    return closest_places


def fill_missing_crime_data(
    places_df: pd.DataFrame, closest_places: list
) -> pd.DataFrame:
    """
    Fills crime data missing values based on the closest places to searched place.
    If the place is poorer than 80% of the searched place it is skipped, as areas
    poorer than 80% may have much higher crime rates to the searched place, so it
    is to avoid data that is way off.
    Args:
        places_df: pd.DataFrame
        closest_places: list
    """
    for place_index, _ in closest_places:
        selected_row = places_df.loc[place_index]
        selected_places_values = selected_row[
            [
                "assault",
                "murder",
                "rape",
                "robbery",
                "burglary",
                "theft",
                "motor_vehicle_theft",
            ]
        ]
        if "no data" not in selected_places_values.values:
            median_income_threshold = (
                selected_row["median_household_income"] * AREA_WEALTH_THRESHOLD
            )
            if (
                places_df.loc[place_index, "median_household_income"]
                > median_income_threshold
            ):
                for col in [
                    "assault",
                    "murder",
                    "rape",
                    "robbery",
                    "burglary",
                    "theft",
                    "motor_vehicle_theft",
                ]:
                    places_df.at[place_index, col] = selected_places_values[col]
                break

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
