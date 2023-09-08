This is my 2nd Project. I am focusing on the data engineering, so data collection and data cleaning. 
Throughout the process of working on this project I realised that I have absolutely 0 interest in front-end, so I decided not to build the webapp.
However, all functions to fit into the webapp are done in the backend, so if anyone wishes it can be added here.

An idea for this project is simple. I was always curious of the United States since I was a kid. I like to check-up different places, reading information how to live in different states, cities, counties, towns... 
<br>
<br>
I am not trying to re-create the already existing websites like Niche.com, or Areavibes.com, as they are full of the information already. I am trying to use their data (and other sources as well) to get as much information about the places to be able to help people to choose their perfect place to live.
<br>
<br>
<b>This app will contain the following</b>:

1. Websites scraper (Niche.com, Areavibes.com).
2. Google API:
   * Restaurants, Pubs, Cafes information and density in the near distance of the areas. 
   * Distance and commute time to work from the area.
3. Climate information for each area.
4. Cleaning the data.
5. Database interaction using SQLAlchemy dataclasses.
6. Functions to provide the data based on the user responses (if the webapp ever comes and the responses are there).
<br>
<br>

<b>NOTE: `config.yml` is not uploaded, as it has api keys and passwords, so you need to create it yourself if you want to use it.</b>

# requirements # 
`bs4` `sqlalchemy` `Google API` `concurrent.futures` `haversine` `meteostat` 


