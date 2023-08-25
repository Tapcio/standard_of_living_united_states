import pandas as pd
import time

from utils.scraping_niche import (
    scrape_get_soup_for_places_and_links,
    get_place_names_and_links_for_page,
    scrape_get_soup_for_place_details,
)
from utils.scraping_niche import (
    scrape_ratings,
    scrape_type_of_place,
    scrape_rent_vs_own,
    scrape_population_and_real_estate,
)
from utils.scraping_niche import scrape_crime_data, scrape_age_groups


def scrape_all_places_and_links(page_start: int, page_finish: int) -> None:
    """
    Triggers scraping places and links and saves to the .csv
    Attributes:
        None
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
