import pytest
from unittest.mock import patch
from utils.googlemaps import (
    get_map_coordinates_by_place_name,
    get_number_of_venues_in_the_area,
)


@pytest.fixture
def mock_client():
    with patch("utils.googlemaps.Client") as mock:
        mock.return_value.geocode.return_value = [
            {"geometry": {"location": {"lat": 37.7749, "lng": -122.4194}}}
        ]
        yield mock


def test_get_map_coordinates_by_place_name(mock_client):
    coordinates = get_map_coordinates_by_place_name("San Francisco, CA")
    assert coordinates["latitude"] == pytest.approx(37.7749, abs=1e-4)
    assert coordinates["longitude"] == pytest.approx(-122.4194, abs=1e-4)


def test_get_map_coordinates_by_place_name_not_found(mock_client):
    mock_client.return_value.geocode.return_value = []
    coordinates = get_map_coordinates_by_place_name("Nonexistent Place")
    assert coordinates["latitude"] is None
    assert coordinates["longitude"] is None


MOCK_RESPONSE = {
    "results": [{"name": "Venue 1"}, {"name": "Venue 2"}],
    "next_page_token": "mock_token",
}


def mock_requests_get(url, params):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if url == "https://maps.googleapis.com/maps/api/place/nearbysearch/json":
        return MockResponse(MOCK_RESPONSE, 200)
    else:
        return MockResponse({}, 404)


@pytest.fixture
def mock_requests():
    with patch("utils.googlemaps.requests.get", side_effect=mock_requests_get):
        yield


def test_get_number_of_venues_in_the_area(mock_requests):
    latitude = 37.7749
    longitude = -122.4194
    keyword = "restaurant"
    radius = 1000

    num_venues = get_number_of_venues_in_the_area(latitude, longitude, keyword, radius)

    assert num_venues == len(MOCK_RESPONSE["results"])
