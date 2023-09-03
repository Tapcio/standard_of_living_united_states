SELECT_PART = """
    SELECT
    f.unique_name, f.school_rating,
    f.families_rating,
    p.state, p.type_of_place, p.link,
    w.median_home_value, w.median_rent, w.median_household_income,
    c.Assault, c.Murder, c.Rape, c.Robbery,
    c.Theft, c.Burglary, c.`Motor Vehicle Theft`,
    a.nightlife_rating, a.restaurants, a.bars, a.cafes,
    af.area_feel, af.population
    FROM families f
    LEFT JOIN places p ON f.unique_name = p.unique_name
    LEFT JOIN wealth w ON f.unique_name = w.unique_name
    LEFT JOIN crimes c ON f.unique_name = c.unique_name
    LEFT JOIN activities a on f.unique_name = a.unique_name
    LEFT JOIN area_feel af on f.unique_name = af.unique_name
    WHERE
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


def create_sql_query(
    state: str,
    median_household_income: int,
    area_feel: list,
    is_woman: bool = False,
    nightlife: bool = False,
    families: bool = False,
    schools: bool = False,
    restaurants: bool = False,
    bars: bool = False,
    cafes: bool = False,
) -> str:
    """
    Creating SQL query based on the user responses.
    Args:
        state: str
        median_household_income: int
        area_feel: list
        is_woman: bool
        nightlife: bool
        families: bool
        schools: bool
        restaurants: bool
        bars: bool
        cafes: bool

    Returns:
        query: str
    """
    state_query = f"p.state = '{state}'"
    median_household_income_query = (
        f"\nAND w.median_household_income < {median_household_income}"
    )
    if restaurants:
        restaurants_query = f"\nAND a.restaurants > 20"
    else:
        restaurants_query = ""

    if cafes:
        cafes_query = f"\nAND a.cafes > 10"
    else:
        cafes_query = ""

    if bars:
        bars_query = f"\nAND a.bars > 10"
    else:
        bars_query = ""
    area_feel_str = ", ".join(f"'{area_type}'" for area_type in area_feel)
    area_feel_query = f"\nAND af.area_feel IN ({area_feel_str})"
    if families or schools or nightlife:
        order_by_query = "\nORDER BY"
    else:
        order_by_query = ""

    if families:
        families_query = FAMILIES_QUERY
    else:
        families_query = ""

    if schools:
        schools_query = SCHOOLS_QUERY
    else:
        schools_query = ""

    if nightlife:
        nightlife_query = NIGHTLIFE_QUERY
    else:
        nightlife_query = ""

    if is_woman:
        sorting_query = "\nc.Rape, c.Assault, c.Murder, c.Robbery, c.Theft DESC"
    else:
        sorting_query = "\nc.Assault, c.Murder, c.Rape, c.Robbery, c.Theft DESC"

    limit_query = "\n LIMIT 10;"
    query = (
        SELECT_PART
        + state_query
        + median_household_income_query
        + restaurants_query
        + cafes_query
        + bars_query
        + area_feel_query
        + order_by_query
        + families_query
        + schools_query
        + nightlife_query
        + sorting_query
        + limit_query
    )
    return query
