import googlemaps
import requests
import time
import concurrent.futures
import pandas as pd
import data_collection_utils.utils_generic as u

data = u.get_config()

GOOGLE_MAPS_API_KEY = data["GOOGLE_MAPS_API_KEY"]


def get_map_coordinates_by_place_name(place_name: str) -> dict:
    """
    Connects to Google API and returns coordinate values using name of the place.
    Returns a dictionary with latitude and longitude
    Args:
        place_name: str
    Returns:
        map_coordinates: dict
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
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
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
                places_df.loc[index, "latitude"] = coordinates["latitude"]
                places_df.loc[index, "longitude"] = coordinates["longitude"]
            except AttributeError:
                places_df.loc[index, "latitude"] = None
                places_df.loc[index, "longitude"] = None
            print(
                f"Coordinates: {coordinates['latitude']}, {coordinates['longitude']} added."
            )
    return places_df


def get_number_of_venues_in_the_area(
    latitude: float, longitude: float, keyword: str, radius=1000
) -> int:
    """
    Searches for nearby places by using map coordinates. Default Radius is 1000 metres.
    Args:
        latitude: float
        longitude: float
        keyword: str
        radius: int = 1000
    Returns:
        number of venues: int
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
        received_data = response.json()

        results = received_data.get("results", [])
        place_type_list.extend(results)

        next_page_token = received_data.get("next_page_token")
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
    Args:
        row: series
        keyword: list
    """
    name = row["name_with_state"]
    latitude = row["latitude"]
    longitude = row["longitude"]

    return get_number_of_venues_in_the_area(name, latitude, longitude, keyword)


def multiprocessing_google_maps(
    places_df: pd.DataFrame, max_workers: int, keywords: list
) -> pd.DataFrame:
    """
    Multiprocessing function. It helps to run the get_number_of_venues_in_the_area() faster.
    Args:
        places_df: pd.DataFrame
        max_workers: int
        keywords: list -> Example: ["restaurant", "bar", "cafe", "supermarket", "park"]
    Returns:
        places_df: pd.DataFrame
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

    return places_df
