from utils.data_cleaning import (
    remove_special_character_school_rating,
    remove_special_character_nightlife_rating,
    create_state_from_link,
)


def test_remove_special_character_school_rating():
    school_rating_input_one = "ÂÂC+ \n"
    school_rating_input_two = "A+"
    school_rating_one = remove_special_character_school_rating(school_rating_input_one)
    school_rating_two = remove_special_character_school_rating(school_rating_input_two)
    assert school_rating_one == "C+"
    assert school_rating_two == "A+"


def test_remove_special_character_nightlife_rating():
    nightlife_rating_input_one = "gradeÂ \n A+ \n"
    nightlife_rating_input_two = " C+"
    nightlife_rating_one = remove_special_character_nightlife_rating(
        nightlife_rating_input_one
    )
    nightlife_rating_two = remove_special_character_nightlife_rating(
        nightlife_rating_input_two
    )
    assert nightlife_rating_one == "A+"
    assert nightlife_rating_two == "C+"


def test_create_state_from_link():
    test_link = "https://www.niche.com/places-to-live/north-bethesda-montgomery-md/"
    state = create_state_from_link(test_link)
    assert state == "MD"
