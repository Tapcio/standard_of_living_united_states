from dataclasses import dataclass


@dataclass
class Places:
    unique_name: str
    name: str
    type_of_place: str
    state: str
    latitude: float
    longitude: float


@dataclass
class Wealth:
    unique_name: str
    median_home_value: int
    median_rent: int
    median_household_value: int


@dataclass
class Weather:
    unique_name: str
    temperature: list[float]
    precipitation: list[float]


@dataclass
class AreaFeel:
    unique_name: str
    area_feel: str
    population: int
    age_list: list[int]


@dataclass
class Crimes:
    unique_name: str
    assault: float
    murder: float
    rape: float
    robbery: float
    burglary: float
    theft: float
    motor_vehicle_theft: float


@dataclass
class Families:
    unique_name: str
    school_rating: str
    families_rating: str


@dataclass
class Activities:
    unique_name: str
    nightlife_rating: str
    restaurants: int
    bars: int
    cafes: int
