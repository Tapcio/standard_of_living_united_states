from bs4 import BeautifulSoup
import re
import requests
import numpy as np

from data_collection_config import (
    VIOLENT_CRIMES_COLUMNS,
    NON_VIOLENT_CRIMES_COLUMNS,
    AGE_GROUP_NAMES,
)


def scrape_get_soup_for_place_details(link: str):
    """
    Returns html soup from the Niche.com Website
    Args:
        link: str
    """
    apikey = "e1054a6fb0009ec0b07a23798a6e63aafa8bc84d"
    params = {
        "url": link,
        "apikey": apikey,
        "js_render": "true",
        "antibot": "true",
        "premium_proxy": "true",
    }
    response = requests.get("https://api.zenrows.com/v1/", params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    return soup


def scrape_get_soup_for_places_and_links(page_number: int) -> BeautifulSoup:
    """
    Returns place names and links to place details from the Niche.com Website
    Args:
        page_number: int
    """
    url = f"https://www.niche.com/places-to-live/search/best-places-to-live/?page={page_number}"
    apikey = "e1054a6fb0009ec0b07a23798a6e63aafa8bc84d"
    params = {
        "url": url,
        "apikey": apikey,
        "js_render": "true",
        "antibot": "true",
        "premium_proxy": "true",
    }
    response = requests.get("https://api.zenrows.com/v1/", params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    return soup


def scrape_ratings(soup: BeautifulSoup) -> dict:
    """
    Scrapes the soup using indices. 0 for school, 3 for nightlife and 4 for families
    Returns dictionary with ratings of school, nightlife and families from Niche.com website.

    Args:
        soup: BeautifulSoup
    """
    ratings_card = soup.find_all("li", class_="ordered__list__bucket__item")
    try:
        school_rating = ratings_card[0].find("div", class_="niche__grade").text[-2:]
    except AttributeError:
        school_rating = np.nan
    try:
        nightlife_rating = ratings_card[3].find("div", class_="niche__grade").text
    except AttributeError:
        nightlife_rating = np.nan
    try:
        families_rating = ratings_card[4].find("div", class_="niche__grade").text
    except AttributeError:
        families_rating = np.nan

    ratings_dictionary = {
        "school_rating": school_rating,
        "nightlife_rating": nightlife_rating,
        "families_rating": families_rating,
    }

    return ratings_dictionary


def scrape_type_of_place(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with type of place from Niche.com website

    Args:
        soup: BeautifulSoup
    """
    # type_of_place_soup = soup.find_all("ul", class_="postcard__attrs")
    try:
        type_of_place = soup.find("li", class_="postcard__attr").text
        type_of_place_dictionary = {"type_of_place": type_of_place}
    except AttributeError:
        type_of_place_dictionary = {"type_of_place": np.nan}
    return type_of_place_dictionary


def scrape_rent_vs_own(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with % of rented and owned properties within the area from Niche.com website
    Args:
        soup: BeautifulSoup
    """
    try:
        rented_percentage_scraped = soup.find(
            "div", class_="fact__table__row__value"
        ).text.replace("%", "")

        rented_percentage = int(rented_percentage_scraped)
        rent_vs_own_dictionary = {
            "rented_percentage": rented_percentage,
            "owned_percentage": 100 - rented_percentage,
        }
    except AttributeError:
        rent_vs_own_dictionary = {
            "rented_percentage": np.nan,
            "owned_percentage": np.nan,
        }
    return rent_vs_own_dictionary


def scrape_population_and_real_estate(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with Population, Median Home Value, Median Rent,
    Median Household Income and Area Feel from Niche.com website
    Args:
        soup: BeautifulSoup
    """

    # Assigning NaN in case value isn't available in the soup
    population = np.nan

    median_home_value = np.nan
    median_rent = np.nan
    area_feel = np.nan
    median_household_income = np.nan
    soup_span_elements = soup.find_all("span")

    for i, span in enumerate(soup_span_elements):
        if span.get_text(strip=True) == "Population":
            population = soup_span_elements[i + 1].get_text(strip=True)
        elif span.get_text(strip=True) == "Median Home Value":
            median_home_value = soup_span_elements[i + 1].get_text(strip=True)
        elif span.get_text(strip=True) == "Median Rent":
            median_rent = soup_span_elements[i + 1].get_text(strip=True)
        elif span.get_text(strip=True) == "Area Feel":
            area_feel = soup_span_elements[i + 1].get_text(strip=True)
        elif span.get_text(strip=True) == "Median Household Income":
            median_household_income = soup_span_elements[i + 1].get_text(strip=True)

    population_and_real_estate_dictionary = {
        "population": population,
        "median_home_value": median_home_value,
        "median_rent": median_rent,
        "median_household_income": median_household_income,
        "area_feel": area_feel,
    }

    return population_and_real_estate_dictionary


def scrape_crime_data(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with Population, Median Home Value, Median Rent,
    Median Household Income and Area Feel from Niche.com website
    Args:
        soup: BeautifulSoup
    """
    crime_categories = [VIOLENT_CRIMES_COLUMNS, NON_VIOLENT_CRIMES_COLUMNS]
    crime_soup = soup.find_all("section", class_="block--one-two-one")
    crime_dict = {}

    for crime in crime_categories:
        pattern = rf'<div class="fact__table__row__label">{crime}</div><div class="fact__table__row__value">([\
        \d.]+)</div>'
        try:
            crime_dict[crime] = re.findall(pattern, str(crime_soup))[0]
        except AttributeError:
            crime_dict[crime] = np.nan
    return crime_dict


def scrape_age_groups(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with age groups% from Niche.com website
    Args:
        soup: BeautifulSoup
    """
    age_groups_dictionary = {}
    try:
        age_groups_soup = soup.find("section", class_="block--one-two").text

        age_groups_soup_split = age_groups_soup.split("%")
        pattern = r"\d+(?:\.\d+)?$"

        for i, age_group_name in enumerate(AGE_GROUP_NAMES):
            percentage = re.search(pattern, age_groups_soup_split[i]).group()
            age_groups_dictionary[age_group_name] = percentage
    except AttributeError:
        pass

    return age_groups_dictionary


def get_place_names_and_links_for_page(soup: BeautifulSoup) -> tuple:
    """
    Returns 2 lists with place names and links from Niche.com
    Args:
        soup: BeautifulSoup
    """
    place_names = []
    links = []

    div_elements = soup.find_all("div", class_="search-result")
    a_elements = soup.find_all("a", class_="search-result__link")

    for div, a in zip(div_elements, a_elements):
        place_name = div.get("aria-label")
        link = a.get("href")

        place_names.append(place_name)
        links.append(link)

    return place_names, links
