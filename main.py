"""
Driver file to be used once front-end is created.

1. Scrape the data(It will take very long time (48-72 hrs))
2. Clean the data
3. Enter personal information
4. Receive the information about the best area to live for you
"""
import click

from drivers.data_collection import get_all_data
from drivers.data_cleaning import clean_all_data
from db_utils.database_operations import return_places


@click.command()
@click.option("--get", is_flag=True, help="Get all data")
@click.option("--clean", is_flag=True, help="Clean all data")
@click.option("--enter-details", is_flag=True, help="Enter your details")
def main(get, clean, enter_details):
    if get:
        get_all_data()
    elif clean:
        clean_all_data()
    elif enter_details:
        state = click.prompt("Enter your state", type=str)
        median_household_income = click.prompt(
            "Enter median household income", type=int
        )
        area_feel = click.prompt(
            "Enter area feel (e.g., quiet, lively)", type=str, default=""
        )
        is_woman = click.confirm("Are you a woman?")
        nightlife = click.confirm("Is nightlife important to you?")
        families = click.confirm("Is living near families important to you?")
        schools = click.confirm("Is living near schools important to you?")
        restaurants = click.confirm("Is living near restaurants important to you?")
        bars = click.confirm("Is living near bars important to you?")
        cafes = click.confirm("Is living near cafes important to you?")

        # Call the return_places function with the entered details
        places = return_places(
            state,
            median_household_income,
            area_feel,
            is_woman,
            nightlife,
            families,
            schools,
            restaurants,
            bars,
            cafes,
        )

        # Print or further process the returned places
        print("Recommended Places:")
        for place in places:
            print(place)
    else:
        click.echo("Please select an option: --get, --clean, or --enter-details")


if __name__ == "__main__":
    main()
