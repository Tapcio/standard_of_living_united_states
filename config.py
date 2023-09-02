import utils.utils_generic as u

data = u.get_config(loc=r"C:\Users\adamz\Documents\standard_of_living\config.yml")

GOOGLE_MAPS_API_KEY = data["GOOGLE_MAPS_API_KEY"]
MYSQL_PASSWORD = data["MYSQL_PASSWORD"]
MYSQL_HOST = data["MYSQL_HOST"]
MYSQL_USER = data["MYSQL_USER"]
MYSQL_DATABASE = data["MYSQL_DATABASE"]

AREA_WEALTH_THRESHOLD = 0.8

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

VIOLENT_CRIMES_COLUMNS = ["Assault", "Murder", "Rape", "Robbery"]

NON_VIOLENT_CRIMES_COLUMNS = ["Burglary", "Theft", "Motor Vehicle Theft"]

ACTIVITIES_COLUMNS = ["unique_name", "nightlife_rating", "restaurants", "bars", "cafes"]

WEALTH_COLUMNS = [
    "unique_name",
    "median_home_value",
    "median_rent",
    "median_household_income",
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

MONTH_TO_SEASON = {
    "December": "Winter",
    "January": "Winter",
    "February": "Winter",
    "March": "Spring",
    "April": "Spring",
    "May": "Spring",
    "June": "Summer",
    "July": "Summer",
    "August": "Summer",
    "September": "Autumn",
    "October": "Autumn",
    "November": "Autumn",
}
