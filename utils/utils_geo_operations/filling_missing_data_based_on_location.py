import haversine as hs
from haversine import Unit
import pandas as pd

AREA_WEALTH_THRESHOLD = 0.8  # This is in case the closest areas are very poor and therefore higher chance of much higher crime


def find_closest_places(places_df: pd.DataFrame, place_index: int) -> list:
    """
    Searches for closest places in the 5km radius. Returns list with sorted
    places by the closest distance.
    Arguments:
        places_df: pd.DataFrame
        place_index: int
    """
    selected_row = places_df.loc[place_index]
    selected_state = selected_row["state"]
    selected_coordinates = (selected_row["latitude"], selected_row["longitude"])

    closest_places = []

    for index, row in places_df.iterrows():
        if index != place_index and row["state"] == selected_state:
            compared_coordinates = (row["latitude"], row["longitude"])
            distance = hs.haversine(
                selected_coordinates, compared_coordinates, unit=Unit.KILOMETERS
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
    Arguments:
        places_df: pd.DataFrame
        closest_places: list
    """
    for place_index, _ in closest_places:
        selected_row = places_df.loc[place_index]
        selected_places_values = selected_row[
            [
                "Assault",
                "Murder",
                "Rape",
                "Robbery",
                "Burglary",
                "Theft",
                "Motor Vehicle Theft",
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
                    "Assault",
                    "Murder",
                    "Rape",
                    "Robbery",
                    "Burglary",
                    "Theft",
                    "Motor Vehicle Theft",
                ]:
                    places_df.at[place_index, col] = selected_places_values[col]
                break

    return places_df