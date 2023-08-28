import pandas as pd
import time
import utils.data_cleaning as dc
from utils.scraping_niche import (
    scrape_get_soup_for_places_and_links,
    get_place_names_and_links_for_page,
    scrape_get_soup_for_place_details,
)
from utils.scraping_areavibes import (
    fill_missing_school_ratings,
    fill_missing_crime_values,
)
from utils.scraping_niche import (
    scrape_ratings,
    scrape_type_of_place,
    scrape_rent_vs_own,
    scrape_population_and_real_estate,
)
from utils.scraping_niche import scrape_crime_data, scrape_age_groups

from utils import database_operations


def scrape_all_places_and_links_niche(page_start: int, page_finish: int):
    """
    Triggers scraping places and links and saves to the .csv
    Args:
        page_start: int
        page_finish: int
    """
    page_range = range(page_start, page_finish)
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


def scrape_all_info_from_place_niche():
    """
    Function calls all scrape functions for Niche.com and merges all dictionaries into one.
    All dictionaries are appended to the DataFrame which is periodically saved into the csv file.
    """
    scraped_data_dataframe = pd.DataFrame()
    places_to_scrape = pd.read_csv("niche_places_and_links.csv")

    for index, row in places_to_scrape.iterrows():
        link = row["link"]

        retries = 0
        while retries < 3:
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
            print(f"Failed to scrape place number {index} after {3} retries.")
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


def data_preprocessing_from_raw(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    It runs all functions from the data_cleaning.py file. These are doing the following:
        1. Cleaning unwanted values.
        2. Transforming to desired datatypes
        3. Creating additional columns based on the data in the dataframe
        4. Reassigning to desired tables in standard_of_living schema
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
    """
    # Cleaning unwanted values
    places_df["school_rating"] = places_df["school_rating"].map(
        dc.remove_special_character_school_rating
    )
    places_df["school_rating"] = places_df["school_rating"].str.strip()
    places_df["nightlife_rating"] = places_df["nightlife_rating"].map(
        dc.remove_special_character_nightlife_rating
    )
    places_df["families_rating"] = places_df["families_rating"].map(
        dc.remove_special_character_families_rating
    )
    places_df = dc.replace_incorrect_type_of_place(places_df)
    # Transforming to desired datatypes
    places_df["population"] = places_df["population"].map(dc.number_to_int)
    places_df["median_home_value"] = places_df["median_home_value"].map(
        dc.number_to_int
    )
    places_df["median_rent"] = places_df["median_rent"].map(dc.number_to_int)
    places_df["median_household_income"] = places_df["median_household_income"].map(
        dc.number_to_int
    )
    places_df = dc.create_rent_to_sell_value_ratio(places_df)
    places_df = dc.fill_missing_rent_and_home_values(places_df)
    places_df.drop("rent_sell_value_ratio", axis=1, inplace=True)
    # Creating additional columns
    places_df["state"] = places_df["link"].map(dc.create_state_from_link)
    places_df["state"] = places_df["state"].map(dc.change_state_abbreviation_to_name)
    places_df.drop(places_df[places_df["state"] == ""].index, inplace=True)
    places_df["name_with_state"] = places_df["link"].map(dc.add_name_with_state)

    return places_df


def scrape_areavibes_and_fill_missing_data(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Driver function that scrapes Areavibes.com for missing data.
    Args:
        places_df: pd.DataFrame
    Returns:
        places_df: pd.DataFrame
    """
    places_df = fill_missing_school_ratings(places_df)
    places_df = fill_missing_crime_values(places_df)

    return places_df


def fill_remaining_crime_data():
    """
    Reads table from the database and assigns to the DataFrame.
    Fills missing values for crime after attempt by Areavibes was taken to fill missing values after previously
    scraping Niche.com
    Saves back to the Database
    """
    crimes = database_operations.load_database_to_dataframe("crimes")
    # In case any values are "no data" (they shouldn't be)
    try:
        crimes = dc.convert_crime_values_to_numeric(crimes)
    except (ValueError, AttributeError):
        pass

    crimes = crimes.apply(dc.add_violent_crimes_ratio)
    crimes = crimes.apply(dc.add_non_violent_crimes_ratio)
    crimes = crimes.apply(dc.fill_nan_values_violent_crimes)
    crimes = crimes.apply(dc.fill_nan_values_non_violent_crimes)

    database_operations.save_dataframe_to_database(crimes, "crimes")
