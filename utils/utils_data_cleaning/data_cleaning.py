import pandas as pd
import numpy as np
import utils.utils_mysql.utils_database_operations as dbops


LINK_LENGTH_FIVE = 5
LINK_LENGTH_FOUR = 4
LINK_LENGTH_THREE = 3

US_AVERAGE_CRIMES = {
    "US_AVERAGE_ASSAULT": 282.7,
    "US_AVERAGE_MURDER": 6.1,
    "US_AVERAGE_RAPE": 40.7,
    "US_AVERAGE_ROBBERY": 135.5,
    "US_AVERAGE_BURGLARY": 500.1,
    "US_AVERAGE_THEFT": 2042.8,
    "US_AVERAGE_MOTOR VEHICLE THEFT": 284.0,
}

CRIMES_COLUMNS = [
    "unique_name",
    "Assault",
    "Murder",
    "Rape",
    "Robbery",
    "Burglary",
    "Theft",
    "Motor Vehicle Theft",
]

VIOLENT_CRIMES_COLUMNS = ["Assault", "Murder", "Rape", "Robbery"]
NON_VIOLENT_CRIMES_COLUMNS = ["Burglary", "Theft", "Motor Vehicle Theft"]

ACTIVITIES_COLUMNS = ["unique_name", "nightlife_rating", "restaurants", "bars", "cafes"]

WEALTH_COLUMNS = [
    "unique_name",
    "median_home_value",
    "median_rent",
    "median_household_income",
]

AREA_FEEL_COLUMNS = [
    "unique_name",
    "area_feel",
    "population",
    "under_ten",
    "ten_to_seventeen",
    "eighteen_to_twentyfour",
    "twentyfive_to_thirtyfour",
    "thirtyfive_to_fourtyfour",
    "fourtyfive_to_fiftyfour",
    "fiftyfive_to_sixtyfour",
    "over_sixtyfive",
]

FAMILIES_COLUMNS = [
    "unique_name",
    "school_rating",
    "families_rating",
    "under_ten",
    "ten_to_seventeen",
    "eighteen_to_twentyfour",
    "twentyfive_to_thirtyfour",
    "thirtyfive_to_fourtyfour",
    "fourtyfive_to_fiftyfour",
    "fiftyfive_to_sixtyfour",
    "over_sixtyfive",
]

PLACES_COLUMNS = [
    "name",
    "link",
    "type_of_place",
    "state",
    "name_with_state",
    "latitude",
    "longitude",
    "unique_name",
]

US_STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}



def remove_special_character_school_rating(school_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Attributes:
        str: school_rating
    """
    return school_rating.replace("Â", "").replace("\xa0", "").strip()


def remove_special_character_nightlife_rating(nightlife_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Attributes:
        str: nightlife_rating
    """
    return nightlife_rating.replace("gradeÂ", "").replace("minus", "-").replace(" ", "")


def remove_special_character_families_rating(families_rating: str) -> str:
    """
    Cleaning of unnecessary characters after scraping Niche.com
    Attributes:
        str: families_rating
    """
    return families_rating.replace("gradeÂ", "").replace("minus", "-").replace(" ", "")


def create_state_from_link(link: str) -> str:
    """
    Gets the state Abbreviation from the link
    Attributes:
        str: link
    """
    split_url = link.split("/")[-2]
    return split_url[-2:].upper()


def number_to_int(number: str) -> int:
    """
    Creates int from the string that contains commas or $ signs
    Attributes:
        str: number
    """
    try:
        return int(number.replace(",", "").replace("$", ""))
    except:
        return "No data available"


def add_missing_type_of_place(link: str) -> str:
    """
    Link has "/n/" part for Neighbourhoods, otherwise it will be Suburb.
    Towns or Cities are captured correctly when scraping, so only creating
    Neighbourhood or Suburb.
    Attributes:
        str: link
    """
    link_split_list = link.split("/")
    if link_split_list[4] == "n":
        place_split = link_split_list[5].split("-")
        return (
            f"Neighborhood in {place_split[-2].capitalize()}, {place_split[-1].upper()}"
        )
    elif link_split_list[4] != "n":
        place_split = link_split_list[4].split("-")
        return f"Suburb of {place_split[-2].capitalize()}, {place_split[-1].upper()}"
    else:
        return "no data"


def change_state_abbreviation_to_name(abbreviation: str) -> str:
    """
    Replicates full State name using dictionary with state names
        str: abbreviation
    Attributes:
        dict: US_STATES
    """
    return US_STATES[abbreviation]


def remove_niche_prefix(link: str) -> str:
    """
    Removes the generic part of the link and returns only the dynamic one that is related to the place
    Attributes:
        str: link
    """
    prefix1 = "https://www.niche.com/places-to-live/"
    prefix2 = "https://www.niche.com/places-to-live/n/"

    if link.startswith(prefix2):
        return link[len(prefix2) :][:-1]
    else:
        return link[len(prefix1) :][:-1]


def create_rent_to_sell_value_ratio(places_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates column with rent to sell ratio
    Attributes:
        DataFrame: places_df
    """
    places_df["median_home_value"] = pd.to_numeric(
        places_df["median_home_value"], errors="coerce"
    )
    places_df["median_rent"] = pd.to_numeric(places_df["median_rent"], errors="coerce")
    places_df["rent_sell_value_ratio"] = (
        places_df["median_rent"] / places_df["median_home_value"]
    )

    return places_df


def add_name_with_state(link: str) -> str:
    """
    Returns name place based on the length of the link's dynamic part.
    Attributes:
        str: link
    """
    link_without_prefix = remove_niche_prefix(link)
    place_name_split = link_without_prefix.split("-")

    if len(place_name_split) == LINK_LENGTH_FIVE:
        place = f"{place_name_split[0].capitalize()} {place_name_split[1].capitalize()}"
        city_or_county = (
            f"{place_name_split[2].capitalize()} {place_name_split[3].capitalize()}"
        )
        state = place_name_split[4].upper()
    elif len(place_name_split) == LINK_LENGTH_FOUR:
        place = f"{place_name_split[0].capitalize()} {place_name_split[1].capitalize()}"
        city_or_county = place_name_split[2].capitalize()
        state = place_name_split[3].upper()
    elif len(place_name_split) == LINK_LENGTH_THREE:
        place = place_name_split[0].capitalize()
        city_or_county = place_name_split[1].capitalize()
        state = place_name_split[2].upper()
    else:
        return f"{place_name_split[0].capitalize()}, {place_name_split[1].upper()}"
    return f"{place}, {city_or_county}, {state}"


def reassign_values_to_separate_dataframes() -> None:
    """
    Reads the places_raw table from the database and creates separate dataframes from it. Then saves them into the database 
    as separate tables:
    - crimes
    - activities
    - area_feel
    - wealth
    - families
    - places
    Attributes:
    """
    places_df = dbops.load_database_to_dataframe("places_raw")
    crimes = places_df[CRIMES_COLUMNS]
    activities = places_df[ACTIVITIES_COLUMNS]
    area_feel = places_df[AREA_FEEL_COLUMNS]
    wealth = places_df[WEALTH_COLUMNS]
    families = places_df[FAMILIES_COLUMNS]
    places = places_df[PLACES_COLUMNS]

    dbops.save_dataframe_to_database(crimes, "crimes")
    dbops.save_dataframe_to_database(activities, "activities")
    dbops.save_dataframe_to_database(area_feel, "area_feel")
    dbops.save_dataframe_to_database(wealth, "wealth")
    dbops.save_dataframe_to_database(families, "families")
    dbops.save_dataframe_to_database(places, "places")


def convert_values_to_numeric(crimes: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms floats into ints and "no data" into NaN, so it is easier to fill missing values later.
    Attributes:
        crimes: pd.DataFrame
    """
    numeric_columns = crimes.columns.difference(['unique_name'])
    crimes[numeric_columns] = crimes[numeric_columns].applymap(lambda x: float(x) if x != "no data" else np.nan)
    
    return crimes


def add_violent_crimes_ratio(row: pd.Series) -> pd.Series:
    non_nan_counter = 0
    crime_ratio_sum = 0

    for column in VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        if np.isnan(crime_value):
            pass
        elif crime_value == 0:
            non_nan_counter += 1
        else:
            crime_ratio_sum += (crime_value/US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"])
            non_nan_counter += 1
            
    if non_nan_counter == 0:
        non_nan_counter = 1
    else:
        pass
    
    return crime_ratio_sum/non_nan_counter


def add_non_violent_crimes_ratio(row: pd.Series) -> pd.Series:
    non_nan_counter = 0
    crime_ratio_sum = 0

    for column in NON_VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        if np.isnan(crime_value):
            pass
        elif crime_value == 0:
            non_nan_counter += 1
        else:
            crime_ratio_sum += (crime_value/US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"])
            non_nan_counter += 1
            
    if non_nan_counter == 0:
        non_nan_counter = 1
    else:
        pass
    
    return crime_ratio_sum/non_nan_counter

def fill_nan_values_violent_crimes(row: pd.Series) -> pd.Series:
    
    for column in VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        
        if np.isnan(crime_value):
            row[column] = row["violent_crime_ratio"] * US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
        else:
            pass
    
    return row


def fill_nan_values_non_violent_crimes(row: pd.Series) -> pd.Series:
    
    for column in NON_VIOLENT_CRIMES_COLUMNS:
        crime_value = row[column]
        
        if np.isnan(crime_value):
            row[column] = row["non_violent_crime_ratio"] * US_AVERAGE_CRIMES[f"US_AVERAGE_{column.upper()}"]
        else:
            pass
    
    return row