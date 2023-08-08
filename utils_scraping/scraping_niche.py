from bs4 import BeautifulSoup
import re
import time
import requests
import pandas as pd

MAX_RETRIES = 3
SCHOOL_INDEX = 0
NIGHTLIFE_INDEX = 3
FAMILIES_INDEX = 4
CRIME_CATEGORIES = [
    "Assault",
    "Murder",
    "Rape",
    "Robbery",
    "Burglary",
    "Theft",
    "Motor Vehicle Theft",
]
AGE_GROUP_NAMES = [
    "under_ten",
    "ten_to_seventeen",
    "eighteen_to_twentyfour",
    "twentyfive_to_thirtyfour",
    "thirtyfive_to_fourtyfour",
    "fourtyfive_to_fiftyfour",
    "fiftyfive_to_sixtyfour",
    "over_sixtyfive",
]


def retry_request(url: str, params: dict, max_retries=MAX_RETRIES) -> None:
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            retries += 1
            time.sleep(5)
    return None


def scrape_get_soup_for_place_details(link: str):
    """
    Returns html soup from the Niche.com Website
    Attributes:
        str: link to the website
    """
    url = link
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


def scrape_get_soup_for_places_and_links(page_number: int) -> BeautifulSoup:
    """
    Returns place names and links to place details from the Niche.com Website
    Attributes:
        int: page_number for the website
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
    Returns dictionary with ratings of school, nightlife and families from Niche.com website

    Attributes:
        soup: html soup
    """
    ratings_card = soup.find_all("li", class_="ordered__list__bucket__item")
    try:
        school_rating = (
            ratings_card[SCHOOL_INDEX].find("div", class_="niche__grade").text[-2:]
        )
    except:
        school_rating = "no data"
    try:
        nightlife_rating = (
            ratings_card[NIGHTLIFE_INDEX].find("div", class_="niche__grade").text
        )
    except:
        nightlife_rating = "no data"
    try:
        families_rating = (
            ratings_card[FAMILIES_INDEX].find("div", class_="niche__grade").text
        )
    except:
        families_rating = "no data"

    ratings_dictionary = {
        "school_rating": school_rating,
        "nightlife_rating": nightlife_rating,
        "families_rating": families_rating,
    }

    return ratings_dictionary


def scrape_type_of_place(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with type of place from Niche.com website

    Attributes:
        soup: html soup
    """
    # type_of_place_soup = soup.find_all("ul", class_="postcard__attrs")
    try:
        type_of_place = soup.find("li", class_="postcard__attr").text
        type_of_place_dictionary = {"type_of_place": type_of_place}
    except:
        type_of_place_dictionary = {"type_of_place": "no data"}
    return type_of_place_dictionary


def scrape_rent_vs_own(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with % of rented and owned properties within the area from Niche.com website
    Attributes:
        soup: html soup
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
    except:
        rent_vs_own_dictionary = {
            "rented_percentage": "no data",
            "owned_percentage": "no data",
        }
    return rent_vs_own_dictionary


def scrape_population_and_real_estate(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with Population, Median Home Value, Median Rent,
    Median Household Income and Area Feel from Niche.com website
    Attributes:
        soup: html soup
    """

    # Assigning "no data" in case value isn't available in the soup
    population = "No data"

    median_home_value = "No data"
    median_rent = "No data"
    area_feel = "No data"
    median_household_income = "No data"
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
    Attributes:
        soup: html soup
    """
    crime_soup = soup.find_all("section", class_="block--one-two-one")
    crime_dict = {}

    for crime in CRIME_CATEGORIES:
        pattern = rf'<div class="fact__table__row__label">{crime}</div><div class="fact__table__row__value">([\d.]+)</div>'
        try:
            crime_dict[crime] = re.findall(pattern, str(crime_soup))[0]
        except:
            crime_dict[crime] = "no data"
    return crime_dict


def scrape_age_groups(soup: BeautifulSoup) -> dict:
    """
    Returns dictionary with age groups% from Niche.com website
    Attributes:
        soup: html soup
    """
    try:
        age_groups_soup = soup.find("section", class_="block--one-two").text
        age_groups_dictionary = {}

        age_groups_soup_split = age_groups_soup.split("%")
        pattern = r"\d+(?:\.\d+)?$"

        for i, age_group_name in enumerate(AGE_GROUP_NAMES):
            percentage = re.search(pattern, age_groups_soup_split[i]).group()
            age_groups_dictionary[age_group_name] = percentage
    except:
        pass

    return age_groups_dictionary


def get_place_names_and_links_for_page(soup: BeautifulSoup) -> list:
    """
    Returns 2 lists with place names and links from Niche.com
    Attributes:
        soup: html soup
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


def scrape_all_places_and_links() -> None:
    """
    Triggers scraping places and links and saves to the .csv
    Attributes:
        None
    """
    page_range = range(105, 380)
    places_and_links_list = []

    for page_number in page_range:
        print(f"Scraping now for page {page_number}.")
        soup = scrape_get_soup_for_places_and_links(page_number)
        place_names, links = get_place_names_and_links_for_page(soup)

        for name, link in zip(place_names, links):
            places_and_links_list.append({"name": name, "link": link})

        df = pd.DataFrame(places_and_links_list)
        df.to_csv("niche_places_and_links.csv", index=False)
        print(f"file saved for page number {page_number}.")

    print("Scraping Done.")


def scrape_all_info_from_place() -> None:
    """
    Function calls all scrape functions for Niche.com and merges all dictionaries into one.
    All dictionaries are appended to the DataFrame which is periodically saved into the csv file.
    Attributes:
        None
    """
    scraped_data_dataframe = pd.DataFrame()
    places_to_scrape = pd.read_csv("niche_places_and_links.csv")

    for index, row in places_to_scrape.iterrows():
        link = row["link"]

        retries = 0
        while retries < MAX_RETRIES:
            try:
                soup = scrape_get_soup_for_place_details(link)
                if soup is not None:
                    break
                else:
                    print("Failed to get soup, retrying...")
            except Exception as e:
                print(f"Scraping  failed: {e}")
            retries += 1
            time.sleep(5)

        if soup is None:
            print(f"Failed to scrape place number {index} after {MAX_RETRIES} retries.")
        else:
            place_name = {"name": row["name"]}
            link = {"link": row["link"]}
            ratings_dictionary = scrape_ratings(soup)
            type_of_place = scrape_type_of_place(soup)
            rent_vs_own = scrape_rent_vs_own(soup)
            population_and_real_estate = scrape_population_and_real_estate(soup)
            crime_data = scrape_crime_data(soup)
            age_groups = scrape_age_groups(soup)

            merged_dict = {
                key: value
                for d in (
                    place_name,
                    link,
                    ratings_dictionary,
                    type_of_place,
                    rent_vs_own,
                    population_and_real_estate,
                    crime_data,
                    age_groups,
                )
                for key, value in d.items()
            }
            merged_scraped_data_dataframe = pd.DataFrame([merged_dict])
            scraped_data_dataframe = pd.concat(
                [scraped_data_dataframe, merged_scraped_data_dataframe],
                ignore_index=True,
            )

            print(f"Scraped place number {index}.")

            if (index + 1) % 10 == 0:
                scraped_data_dataframe.to_csv("niche_all_scraped_data.csv", index=False)
                print(f"Saved {index + 1} rows of scraped data.")

    scraped_data_dataframe.to_csv("niche_all_scraped_data.csv", index=False)
    print(
        f"All data has been scraped and saved into the folder. Total rows: {len(scraped_data_dataframe)}"
    )
