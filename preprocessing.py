import pandas as pd
import numpy as np

from utils import database_operations


def load_data_and_merge():
    """

    Returns:

    """
    weather = database_operations.load_database_to_dataframe("weather")
    crimes = database_operations.load_database_to_dataframe("crimes")
    activities = database_operations.load_database_to_dataframe("activities")
    area_feel = database_operations.load_database_to_dataframe("area_feel")
    wealth = database_operations.load_database_to_dataframe("wealth")
    families = database_operations.load_database_to_dataframe("families")

    dataframes = [area_feel, wealth, crimes, activities, families, weather]

    places_df = dataframes[0]
    for df in dataframes[1:]:
        places_df = places_df.merge(df, on="unique_name", how="outer")

    return places_df


def fill_missing_school_ratings(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing school ratings for purpose of model fitting. It assigns the same rating to the school rating as
    the families_rating. This will create some small discrepancies, but on the most of the occasions the ratings are similar for both.
    Args:
        places_df: pd.DataFrame

    Returns:
        places_df: pd.DataFrame
    """
    for index, row in places_df.iterrows():
        if row["school_rating"] == "no data":
            places_df.loc[index, "school_rating"] = row["families_rating"]
        else:
            places_df.loc[index, "school_rating"] = row["school_rating"]

    return places_df


def drop_places_with_missing_weather_data(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops places with missing temperature. There is very little places without data, so for the purpose
    of fitting the data into the model we are dropping them.
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
    """
    weather_df = database_operations.load_database_to_dataframe("weather")
    weather_data_cols = weather_df.columns[1:]

    for col in weather_data_cols:
        for index, row in places_df.iterrows():
            if pd.isna(row[col]):
                places_df.drop(index, inplace=True)

    return places_df


# places["score"] = places.index + 1
