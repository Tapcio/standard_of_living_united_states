import googlemaps
import requests
import yaml
import time
import concurrent.futures
import pandas as pd

with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

GOOGLE_MAPS_API_KEY = config["API_KEY"]


def get_map_coordinates_by_place_name(place_name: str) -> dict:
    """
    Connects to Google API and returns coordinate values using name of the place.
    Returns a dictionary with latitude and longitude
    Attributes:
        str: place_name
    """
    gmaps = googlemaps.Client(GOOGLE_MAPS_API_KEY)
    geocode_result = gmaps.geocode(place_name)

    if geocode_result:
        location = geocode_result[0]["geometry"]["location"]
        latitude = location["lat"]
        longitude = location["lng"]
        return {"latitude": latitude, "longitude": longitude}
    else:
        return {"latitude": None, "longitude": None}


def add_missing_map_coordinates_to_dataframe(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds coordinates to the DataFrame if they are missing.
    Attributes:
        DataFrame: places_df
    """
    if "latitude" not in places_df.columns:
        places_df["latitude"] = None
    if "longitude" not in places_df.columns:
        places_df["longitude"] = None

    for index, row in places_df.iterrows():
        if pd.isnull(row["latitude"]) or pd.isnull(row["longitude"]):
            print(
                f"Asking Google for coordinates for: {places_df['name_with_state'][index]}"
            )
            try:
                coordinates = get_map_coordinates_by_place_name(row["name_with_state"])
                places_df.at[index, "latitude"] = coordinates["latitude"]
                places_df.at[index, "longitude"] = coordinates["longitude"]
            except:
                places_df.at[index, "latitude"] = None
                places_df.at[index, "longitude"] = None
            print(
                f"Coordinates: {coordinates['latitude']}, {coordinates['longitude']} added."
            )
    return places_df


def get_number_of_venues_in_the_area(
    name: str, latitude: float, longitude: float, keyword: str, radius=1000
) -> int:
    """
    Searches for nearby places by using map coordinates. Default Radius is 1000 metres.
    Attributes:
        str: name
        float: latitude
        float: longitude
        str: keyword
    """
    base_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{latitude},{longitude}",
        "radius": radius,
        "key": GOOGLE_MAPS_API_KEY,
        "keyword": keyword,
    }
    place_type_list = []

    while True:
        response = requests.get(base_url, params=params)
        data = response.json()

        results = data.get("results", [])
        place_type_list.extend(results)

        next_page_token = data.get("next_page_token")
        time.sleep(
            3
        )  # Necessary to return max values. If smaller than 3 seconds then it may return not enough values.
        if not next_page_token:
            break

        params["pagetoken"] = next_page_token

    return len(place_type_list)


def process_row(row: pd.Series, keyword: list):
    """
    Helper function that is handling the processing of each individual row from the DataFrame.
    It receives "row" which is the Series of an entire row, then it runs the function with selected
    values from the row.
    Attributes:
        Series: row
        list: keyword
    """
    name = row["name_with_state"]
    latitude = row["latitude"]
    longitude = row["longitude"]
    return get_number_of_venues_in_the_area(name, latitude, longitude, keyword=keyword)


def main(places_df: pd.DataFrame, max_workers: int, keywords: list) -> None:
    """
    Multiprocessing function. It helps to run the get_number_of_venues_in_the_area() faster.
    Doesn't return anything. Instead it is updating the dataframe that was passed to the function.
    Attributes:
        DataFrame: places_df
        int: max_workers
        list: keywords -> example: ["restaurant", "bar", "cafe", "supermarket", "park"]
    """

    for keyword in keywords:
        column_name = f"{keyword}s"  # Create column name based on keyword
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(
                executor.map(
                    lambda row: process_row(row[1], keyword), places_df.iterrows()
                )  # row[1] -> element[1] is the row. element[0] is just the index value.
            )

        places_df[column_name] = pd.Series(results)
