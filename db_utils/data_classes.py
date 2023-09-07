from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Places(Base):
    __tablename__ = "places"
    unique_name = Column(String, primary_key=True)
    name = Column(String)
    type_of_place = Column(String)
    state = Column(String)
    link = Column(String)


class Wealth(Base):
    __tablename__ = "wealth"
    unique_name = Column(String, primary_key=True)
    median_home_value = Column(Integer)
    median_rent = Column(Integer)
    median_household_income = Column(Integer)


class Weather(Base):
    __tablename__ = "weather"
    unique_name = Column(String, primary_key=True)
    temp_first_quarter = Column(Float)
    temp_second_quarter = Column(Float)
    temp_third_quarter = Column(Float)
    temp_fourth_quarter = Column(Float)
    prcp_first_quarter = Column(Float)
    prcp_second_quarter = Column(Float)
    prcp_third_quarter = Column(Float)
    prcp_fourth_quarter = Column(Float)


class AreaFeel(Base):
    __tablename__ = "area_feel"
    unique_name = Column(String, primary_key=True)
    area_feel = Column(String)
    population = Column(Integer)
    under_ten = Column(Integer)
    ten_to_seventeen = Column(Integer)
    eighteen_to_twentyfour = Column(Integer)
    twentyfive_to_thirtyfour = Column(Integer)
    thirtyfive_to_fourtyfour = Column(Integer)
    fourtyfive_to_fiftyfour = Column(Integer)
    fiftyfive_to_sixtyfour = Column(Integer)
    over_sixtyfive = Column(Integer)


class Crimes(Base):
    __tablename__ = "crimes"
    unique_name = Column(String, primary_key=True)
    assault = Column(Float)
    murder = Column(Float)
    rape = Column(Float)
    robbery = Column(Float)
    burglary = Column(Float)
    theft = Column(Float)
    motor_vehicle_theft = Column(Float)


class Families(Base):
    __tablename__ = "families"
    unique_name = Column(String, primary_key=True)
    school_rating = Column(String)
    families_rating = Column(String)


class Activities(Base):
    __tablename__ = "activities"
    unique_name = Column(String, primary_key=True)
    nightlife_rating = Column(String)
    restaurants = Column(Integer)
    bars = Column(Integer)
    cafes = Column(Integer)
