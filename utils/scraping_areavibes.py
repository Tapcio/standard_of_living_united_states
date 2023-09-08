"""
Functions used to scrape Areavibes.com website.
To be used after Niche.com was scraped, as those are just missing data fillers.
"""
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from config import US_AVERAGE_CRIMES
from typing import Optional


def scrape_areavibes_website_for_soup(url: str) -> BeautifulSoup:
    """
    Takes the url to the website and returns html soup
    Args:
        url: str
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    }
    request_soup = requests.get(url, headers=headers)
    soup = BeautifulSoup(request_soup.content, "html.parser")
    return soup


def scrape_missing_school_ratings(soup: BeautifulSoup) -> str:
    """
    Scrapes the school ratings in the area.
    Args:
        soup: BeautifulSoup
    """
    school_rating = soup.find("div", class_="sfs__score").text

    return school_rating


def scrape_missing_crime_data(soup: BeautifulSoup) -> tuple:
    """
    Scrapes the crime % of the annual averages for each place.
    Args:
        soup: BeautifulSoup
    """
    violent_crime_soup = soup.find("div", class_="sfs__fact b")
    violent_crime_element = violent_crime_soup.find("span", class_="circle-text")

    if violent_crime_element:
        violent_crime_percentage = violent_crime_element.get_text(strip=True)
    else:
        violent_crime_percentage = np.nan

    property_crime = soup.find("div", class_="sfs__fact c")
    property_crime_element = property_crime.find("span", class_="circle-text")

    if property_crime_element:
        property_crime_percentage = property_crime_element.get_text(strip=True)
    else:
        property_crime_percentage = np.nan

    violent_crime_percentage = int(
        violent_crime_percentage.replace("%", "").replace(",", "")
    )
    property_crime_percentage = int(
        property_crime_percentage.replace("%", "").replace(",", "")
    )

    if (violent_crime_percentage / 100 > 1) or (property_crime_percentage / 100 > 1):
        return 0, 0
    else:
        return violent_crime_percentage / 100, property_crime_percentage / 100


def create_link_first_attempt(
    type_of_place: str,
    name: str,
    name_with_state: str,
    subdirectory: Optional[str] = "",
) -> str | BeautifulSoup:
    """
    Creates a link based on the details of the place. 1st attempt, as combinations of dynamic part of the link
    might be sometimes different.
    Args:
        subdirectory:
        type_of_place: str
        name: str
        name_with_state: str
    Returns:
        str | BeautifulSoup
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

    link = f"{link_beginning}{link_end}/{subdirectory}"
    print(link)
    return scrape_areavibes_website_for_soup(link)


def create_link_second_attempt(
    type_of_place: str,
    name: str,
    name_with_state: str,
    subdirectory: Optional[str] = "",
) -> str | BeautifulSoup:
    """
    Creates a link based on the details of the place. 2nd attempt, as combinations of dynamic part of the link
    might be sometimes different.
    Args:
        type_of_place: str
        name: str
        name_with_state: str
        subdirectory: str
    Returns:
        str | BeautifulSoup
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
    link = f"{link_beginning}{link_end}/{subdirectory}"
    print(link)
    return scrape_areavibes_website_for_soup(link)


def fill_missing_school_ratings(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing school rating if it was not found on Niche.com
    "us" is the string that is returned from Niche.com if no rating
    was found
    Args:
        places_df: pd.DataFrame

    Returns:
        places_df: pd.DataFrame
    """
    for index, row in places_df.iterrows():
        if (row["school_rating"] == "us") or (row["school_rating"] == np.nan):
            type_of_place = row["type_of_place"]
            name = row["name"]
            name_with_state = row["name_with_state"]

            print(f"Processing {name_with_state}")

            try:
                link = create_link_first_attempt(
                    type_of_place, name, name_with_state, "schools"
                )
                school_rating = scrape_missing_school_ratings(link)
                places_df.loc[index, "school_rating"] = school_rating
            except Exception as e:
                print(f"Processing failed with error: {e}")
                try:
                    link = create_link_second_attempt(
                        type_of_place, name, name_with_state, "schools"
                    )
                    school_rating = scrape_missing_school_ratings(link)
                    places_df.loc[index, "school_rating"] = school_rating
                except Exception as e:
                    print(f"Secondary processing failed with error: {e}")
                    places_df.loc[index, "school_rating"] = np.nan

    return places_df


def fill_missing_crime_values(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing crime values if data is missing. Values are based on the scraped
    percentages of average crimes in US. Then multiplied times the average to get into
    the final value.

    First and Second Attempts are using different combinations of links, as for example
    some areas might be considered as suburbs, or neighbourhoods in Niche.com, or Areavibes,
    so this is to have the additional try if the area isn't found the first time.
    Args:
        places_df: pd.DataFrame
    """
    for index, row in places_df.iterrows():
        if row["Assault"] == np.nan:
            print(f"Processing {places_df['name_with_state'][index]}")

            try:
                (
                    violent_crime_percentage,
                    property_crime_percentage,
                ) = scrape_missing_crime_data(
                    create_link_first_attempt(
                        row["type_of_place"],
                        row["name"],
                        row["name_with_state"],
                        "crime",
                    )
                )
                places_df.at[index, "assault"] = (
                    violent_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_ASSAULT"]
                )
                places_df.at[index, "murder"] = (
                    violent_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_MURDER"]
                )
                places_df.at[index, "rape"] = (
                    violent_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_RAPE"]
                )
                places_df.at[index, "robbery"] = (
                    violent_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_ROBBERY"]
                )
                places_df.at[index, "burglary"] = (
                    property_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_BURGLARY"]
                )
                places_df.at[index, "theft"] = (
                    property_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_THEFT"]
                )
                places_df.at[index, "motor_vehicle_theft"] = (
                    property_crime_percentage
                    * US_AVERAGE_CRIMES["US_AVERAGE_MOTOR_VEHICLE_THEFT"]
                )

            except AttributeError:
                print("Processing failed. Retrying...")

                try:
                    (
                        violent_crime_percentage,
                        property_crime_percentage,
                    ) = scrape_missing_crime_data(
                        create_link_second_attempt(
                            row["type_of_place"],
                            row["name"],
                            row["name_with_state"],
                            "crime",
                        )
                    )
                    places_df.at[index, "assault"] = (
                        violent_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_ASSAULT"]
                    )
                    places_df.at[index, "murder"] = (
                        violent_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_MURDER"]
                    )
                    places_df.at[index, "rape"] = (
                        violent_crime_percentage * US_AVERAGE_CRIMES["US_AVERAGE_RAPE"]
                    )
                    places_df.at[index, "robbery"] = (
                        violent_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_ROBBERY"]
                    )
                    places_df.at[index, "burglary"] = (
                        property_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_BURGLARY"]
                    )
                    places_df.at[index, "theft"] = (
                        property_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_THEFT"]
                    )
                    places_df.at[index, "motor_vehicle_theft"] = (
                        property_crime_percentage
                        * US_AVERAGE_CRIMES["US_AVERAGE_MOTOR_VEHICLE_THEFT"]
                    )

                except AttributeError:
                    print("Processing failed.")

    return places_df


def assign_livability_score_to_all_places(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Scrapes Areavibes to get the Livability Score for each place.
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
    """
    for index, row in places_df.iterrows():
        try:
            soup = create_link_first_attempt(
                row["type_of_place"], row["name"], row["name_with_state"]
            )
            livability_score = soup.find("span", class_="cw-score-numerator").get_text(
                strip=True
            )
            places_df.loc[index, "score"] = livability_score
        except (ValueError, KeyError, AttributeError):
            print("Processing failed. Retrying...")

            try:
                soup = create_link_second_attempt(
                    row["type_of_place"], row["name"], row["name_with_state"]
                )
                livability_score = soup.find(
                    "span", class_="cw-score-numerator"
                ).get_text(strip=True)
                places_df.loc[index, "score"] = livability_score
            except (ValueError, KeyError, AttributeError):
                print("Processing failed.")

    return places_df
