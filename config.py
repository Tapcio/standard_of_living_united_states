import utils.utils_generic as u

data = u.get_config(
    loc=r"C:\Users\adamz\PycharmProjects\standard_of_living_united_states\config.yml"
)
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
    "US_AVERAGE_MOTOR_VEHICLE_THEFT": 284.0,
}

CRIMES_COLUMNS = [
    "unique_name",
    "assault",
    "murder",
    "rape",
    "robbery",
    "burglary",
    "theft",
    "motor_vehicle_theft",
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

VIOLENT_CRIMES_COLUMNS = ["assault", "murder", "rape", "robbery"]

NON_VIOLENT_CRIMES_COLUMNS = ["burglary", "theft", "motor_vehicle_theft"]

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

SELECT_PART = r"""
    SELECT
    f.unique_name
    FROM families f
    LEFT JOIN places p ON f.unique_name = p.unique_name
    LEFT JOIN wealth w ON f.unique_name = w.unique_name
    LEFT JOIN crimes c ON f.unique_name = c.unique_name
    LEFT JOIN activities a on f.unique_name = a.unique_name
    LEFT JOIN areafeel af on f.unique_name = af.unique_name
"""

FAMILIES_QUERY = """
    CASE
        WHEN f.families_rating = 'A+' THEN 1
        WHEN f.families_rating = 'A' THEN 2
        WHEN f.families_rating = 'A-' THEN 3
        WHEN f.families_rating = 'B+' THEN 4
        WHEN f.families_rating = 'B' THEN 5
        ELSE 6
    END,
"""

SCHOOLS_QUERY = """
    CASE
        WHEN f.school_rating = 'A+' THEN 1
        WHEN f.school_rating = 'A' THEN 2
        WHEN f.school_rating = 'A-' THEN 3
        WHEN f.school_rating = 'B+' THEN 4
        WHEN f.school_rating = 'B' THEN 5
    ELSE 6
END,
"""

NIGHTLIFE_QUERY = """
    CASE
        WHEN a.nightlife_rating = 'A+' THEN 1
        WHEN a.nightlife_rating = 'A' THEN 2
        WHEN a.nightlife_rating = 'A-' THEN 3
        WHEN a.nightlife_rating = 'B+' THEN 4
        WHEN a.nightlife_rating = 'B' THEN 5
    ELSE 6
END,
"""
