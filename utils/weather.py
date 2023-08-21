from datetime import datetime
from meteostat import Monthly, Point
import pandas as pd

MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def return_average_monthly_temp(latitude: float, longitude: float) -> pd.DataFrame:
    """
    Returns average temperature and precipitation for each month based on
    the data from 1 Jan 2000 to 31 Dec 2022.
    Arguments:
        latitude: float
        longitude: float
    """
    start = datetime(2000, 1, 1)
    end = datetime(2022, 12, 31)
    station = Point(latitude, longitude)

    data = Monthly(station, start, end).fetch()

    monthly_mean_data = data.groupby([data.index.month, data.index.year]).mean()
    overall_monthly_mean_data = monthly_mean_data.groupby(level=0).mean()
    overall_monthly_mean_data.index = MONTH_NAMES

    return overall_monthly_mean_data[["tavg", "prcp"]]


def create_temperature_df(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds columns to the DataFrame with average temp and precipitation for each month.
    Arguments:
    places_df: pd.DataFrame
    """
    temp_df = pd.DataFrame()

    for index, row in places_df.iterrows():
        latitude = row["latitude"]
        longitude = row["longitude"]

        try:
            temp_data = return_average_monthly_temp(latitude, longitude)
        except Exception as e:
            continue

        for month in temp_data.index:
            column_name = f"{month}_tavg"
            temp_df.loc[index, column_name] = temp_data.loc[month, "tavg"]

        for month in temp_data.index:
            column_name = f"{month}_prcp"
            temp_df.loc[index, column_name] = temp_data.loc[month, "prcp"]

        temp_df["unique_name"] = places_df["unique_name"]
    return temp_df

def fill_missing_weather_values(places_df: pd.DataFrame) -> pd.DataFrame:
    
    for i, month in enumerate(MONTH_NAMES):
        tavg_col = f"{month}_tavg"
        prcp_col = f"{month}_prcp"
        
        tavg_avg_col = f"{month}_tavg_avg"
        prcp_avg_col = f"{month}_prcp_avg"
        
        prev_month = MONTH_NAMES[(i-1) % 12]
        next_month = MONTH_NAMES[(i+1) % 12]
        
        places_df[tavg_avg_col] = (places_df[f"{prev_month}_tavg"] + places_df[f"{next_month}_tavg"]) / 2
        places_df[prcp_avg_col] = (places_df[f"{prev_month}_prcp"] + places_df[f"{next_month}_prcp"]) / 2
        
        places_df[tavg_col].fillna(places_df[tavg_avg_col], inplace=True)
        places_df[prcp_col].fillna(places_df[prcp_avg_col], inplace=True)
        
        places_df.drop(columns=[tavg_avg_col, prcp_avg_col], inplace=True)
    
    return places_df