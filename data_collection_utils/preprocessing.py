import pandas as pd

from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import IterativeImputer
from sklearn import linear_model
from typing import Callable

from data_collection_utils import database_operations


def fill_missing_school_ratings(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing school ratings for purpose of model fitting. It assigns the same rating to the school rating as the
    families_rating. This will create some small discrepancies, but on the most of the occasions the ratings are
    similar for both. Args: places_df: pd.DataFrame

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


def impute_missing_values_knn(
    database_name: str, column_first: int, column_last: int, n_neighbors=5
):
    """
    Reads chosen database.
    Imputing missing temperature values using KNN Imputer.
    Saves the values back to the database.
    Args:
        database_name: str
        column_first: int
        column_last: int
        n_neighbors: int = 5 (default value)
    """
    df = database_operations.load_database_to_dataframe(database_name)
    columns = df.columns[column_first:column_last]
    df_to_impute = df[columns].copy()

    min_value = df_to_impute.min().min()
    max_value = df_to_impute.max().max()
    scaler = MinMaxScaler(feature_range=(min_value, max_value))
    df_to_impute_scaled = pd.DataFrame(
        scaler.fit_transform(df_to_impute), columns=columns
    )

    knn_imputer = KNNImputer(
        n_neighbors=n_neighbors, weights="uniform", metric="nan_euclidean"
    )
    df_imputed = pd.DataFrame(
        knn_imputer.fit_transform(df_to_impute_scaled), columns=columns
    )
    df[columns] = df_imputed
    database_operations.save_dataframe_to_database(df, database_name)


def impute_missing_values_mice(
    database_name: str,
    column_first: int,
    column_last: int,
    estimator: Callable = linear_model.BayesianRidge(),
    n_nearest_features: int = None,
    imputation_order: str = "ascending",
):
    """
    Reads chosen database.
    Imputing missing temperature values using MICE.
    Saves the values back to the database.
    Args:
        database_name: str
        column_first: int
        column_last: int
        estimator: Callable = linear_model.BayesianRidge()
        n_nearest_features: int = None
        imputation_order: str = "ascending"
    """
    df = database_operations.load_database_to_dataframe(database_name)
    columns = df.columns[column_first:column_last]
    df_to_impute = df[columns].copy()

    mice_imputer = IterativeImputer(
        estimator=estimator,
        n_nearest_features=n_nearest_features,
        imputation_order=imputation_order,
    )
    df_mice_imputed = pd.DataFrame(
        mice_imputer.fit_transform(df_to_impute), columns=columns
    )
    df[columns] = df_mice_imputed
    database_operations.save_dataframe_to_database(df, database_name)


def weather_bucketing_per_season():
    """
    Calculate average of each quarter of the year, so the weather table is shrunk to fewer columns.
    """
    weather = database_operations.load_database_to_dataframe("weather")
    weather_columns = weather.columns[1:]

    new_data = []
    for index, row in weather.iterrows():
        means = []
        for i in range(0, len(weather_columns), 3):
            mean = sum(row[weather_columns[i : i + 3]]) / 3
            means.append(mean)
        new_data.append(means)

    new_columns = [
        "temp_first_quarter",
        "temp_second_quarter",
        "temp_third_quarter",
        "temp_fourth_quarter",
        "prcp_first_quarter",
        "prcp_second_quarter",
        "prcp_third_quarter",
        "prcp_fourth_quarter",
    ]

    new_weather = pd.DataFrame(new_data, columns=new_columns)
    new_weather.insert(0, "unique_name", weather["unique_name"])
    database_operations.save_dataframe_to_database(new_weather, "weather")
