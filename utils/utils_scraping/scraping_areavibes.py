from bs4 import BeautifulSoup
import requests
import pandas as pd

US_AVERAGE_ASSAULT = 282.7
US_AVERAGE_MURDER = 6.1
US_AVERAGE_RAPE = 40.7
US_AVERAGE_ROBBERY = 135.5
US_AVERAGE_BURGLARY = 500.1
US_AVERAGE_THEFT = 2042.8
US_AVERAGE_MOTOR_VEHICLE_THEFT = 284

def scrape_missing_crime_data(url_crime: str) -> list:
    """
    Scrapes the crime % of the annual averages for each place.
    Attributes:
        url_crime
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    request_crime = requests.get(url_crime, headers=headers)
    soup = BeautifulSoup(request_crime.content, "html.parser")
    violent_crime = soup.find("div", class_="sfs__fact b")
    violent_crime_element = violent_crime.find("span", class_="circle-text")

    if violent_crime_element:
        violent_crime_percentage = violent_crime_element.get_text(strip=True)
    else:
        violent_crime_percentage = "no data"

    property_crime = soup.find("div", class_="sfs__fact c")
    property_crime_element = property_crime.find("span", class_="circle-text")

    if property_crime_element:
        property_crime_percentage = property_crime_element.get_text(strip=True)
    else:
        property_crime_percentage = "no data"

    violent_crime_percentage = int(
        violent_crime_percentage.replace("%", "").replace(",", "")
    )
    property_crime_percentage = int(
        property_crime_percentage.replace("%", "").replace(",", "")
    )

    if (violent_crime_percentage / 100 > 1) or (property_crime_percentage / 100 > 1):
        return 0, 0
    else:
        return [violent_crime_percentage / 100, property_crime_percentage / 100]


def create_link_for_crime_data_first_attempt(type_of_place: str, name: str, name_with_state: str) -> str:
    """
    Creates a link based on the details of the place. 1st attempt, as combinations of dynamic part of the link 
    might be sometimes different.
    Attributes:
        str: type_of_place
        str: name
        str: name_with_state
    """
    link_beginning = r"https://www.areavibes.com/"

    if type_of_place.split(" ")[0] == "Neighborhood":
        place_split = type_of_place[16:].split(", ")
        place = place_split[0].replace(" ", "+")
        neighborhood = name.replace(" ", "+")
        link_end = f"{place}-{place_split[-1]}/{neighborhood}"
    elif type_of_place.split(" ")[0] == "Suburb":
        place_split = type_of_place.split(", ")
        suburb = name.replace(" ", "+")
        link_end = f"{suburb}-{place_split[-1]}"
    elif type_of_place.split(" ")[0] == "City" or type_of_place.split(" ")[0] == "Town":
        place_split = name_with_state.split(", ")
        city_or_town = name.replace(" ", "+")
        link_end = f"{city_or_town}-{place_split[-1]}"
    else:
        return "Couldn't work-out the link."
    return f"{link_beginning}{link_end}/crime"


def create_link_for_crime_data_second_attempt(type_of_place: str, name: str, name_with_state: str):
    """
    Creates a link based on the details of the place. 2st attempt, as combinations of dynamic part of the link 
    might be sometimes different.
    Attributes:
        str: type_of_place
        str: name
        str: name_with_state
    """
    link_beginning = r"https://www.areavibes.com/"

    if type_of_place.split(" ")[0] == "Neighborhood":
        place_split = type_of_place[16:].split(", ")
        place = place_split[0].replace(" ", "+")
        neighborhood = name.replace(" ", "+")
        link_end = f"{place}-{place_split[-1]}/{neighborhood}"
    elif type_of_place.split(" ")[0] == "Suburb":
        name_with_state_split = name_with_state.split(", ")
        suburb = name.replace(" ", "+")
        link_end = f"{suburb}-{name_with_state_split[-1]}"
    elif type_of_place.split(" ")[0] == "City" or type_of_place.split(" ")[0] == "Town":
        place_split = name_with_state.split(", ")
        city_or_town = name.replace(" ", "+")
        link_end = f"{city_or_town}-{place_split[-1]}"
    else:
        return "Couldn't work-out the link."
    return f"{link_beginning}{link_end}/crime"


def fill_missing_crime_values(places_df: pd.DataFrame):
    """
    Fills missing crime values if data is missing. Values are based on the scraped 
    percentages of average crimes in US. Then multiplied times the average to get into
    the final value.
    Attributes:
        DataFrame: places_df
    """
    for index, row in places_df.iterrows():
        if row["Assault"] == "no data":
            print(f"Processing {places_df['name_with_state'][index]}")

            try:
                (
                    violent_crime_percentage,
                    property_crime_percentage,
                ) = scrape_missing_crime_data(
                    create_link_for_crime_data_first_attempt(
                        row["type_of_place"], row["name"], row["name_with_state"]
                    )
                )
                places_df.at[index, "Assault"] = (
                    violent_crime_percentage * US_AVERAGE_ASSAULT
                )
                places_df.at[index, "Murder"] = (
                    violent_crime_percentage * US_AVERAGE_MURDER
                )
                places_df.at[index, "Rape"] = violent_crime_percentage * US_AVERAGE_RAPE
                places_df.at[index, "Robbery"] = (
                    violent_crime_percentage * US_AVERAGE_ROBBERY
                )
                places_df.at[index, "Burglary"] = (
                    property_crime_percentage * US_AVERAGE_BURGLARY
                )
                places_df.at[index, "Theft"] = (
                    property_crime_percentage * US_AVERAGE_THEFT
                )
                places_df.at[index, "Motor Vehicle Theft"] = (
                    property_crime_percentage * US_AVERAGE_MOTOR_VEHICLE_THEFT
                )

            except AttributeError:
                print("Processing failed. Retrying...")

                try:
                    (
                        violent_crime_percentage,
                        property_crime_percentage,
                    ) = scrape_missing_crime_data(
                        create_link_for_crime_data_second_attempt(
                            row["type_of_place"], row["name"], row["name_with_state"]
                        )
                    )
                    places_df.at[index, "Assault"] = (
                        violent_crime_percentage * US_AVERAGE_ASSAULT
                    )
                    places_df.at[index, "Murder"] = (
                        violent_crime_percentage * US_AVERAGE_MURDER
                    )
                    places_df.at[index, "Rape"] = (
                        violent_crime_percentage * US_AVERAGE_RAPE
                    )
                    places_df.at[index, "Robbery"] = (
                        violent_crime_percentage * US_AVERAGE_ROBBERY
                    )
                    places_df.at[index, "Burglary"] = (
                        property_crime_percentage * US_AVERAGE_BURGLARY
                    )
                    places_df.at[index, "Theft"] = (
                        property_crime_percentage * US_AVERAGE_THEFT
                    )
                    places_df.at[index, "Motor Vehicle Theft"] = (
                        property_crime_percentage * US_AVERAGE_MOTOR_VEHICLE_THEFT
                    )

                except AttributeError:
                    print("Processing failed.")

    return places_df