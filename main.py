import os
import pandas as pd
import requests
import configparser
import numpy as np
import pandas as pd
from opencage.geocoder import OpenCageGeocoder
from event import Event

# ConfigParser
config = configparser.ConfigParser()
config.read('config/meetup.cfg')

# Environ variables for API KEY
os.environ['MEETUP_API_KEY'] = config['MEETUP']['API_KEY']
os.environ['OPENCAGE_KEY'] = config['OPENCAGE']['KEY']

# Initialize OpenCage Geocode
geocode = OpenCageGeocoder(os.environ['OPENCAGE_KEY'])

# Function for request events

# function for request meetup API


def request_event(topic, country=None, url_path):
    '''
    Function that make a request to meetup api and return a json from all events filtered by topic and country

    Args:
        topic (string): Topic of the events
        country (string): Country of the events
        url_path (string): Url for meetup API https://www.meetup.com/es-ES/meetup_api/docs/
    '''
    default_args = dict(
        country=country,
        topic=topic,
        key=os.environ['MEETUP_API_KEY']
    )

    r = requests.get(url_path, params=default_args)
    results = r.text
    return json.loads(results)


# url meetup request
url_meetup_api = "https://api.meetup.com/2/open_events"
# get list of events by topic and country
list_events = request_event(topic="Python", country=None, url_meetup_api)
# get data from json response
data = list_events['results']
