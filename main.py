import os
import requests
import configparser
import numpy as np
import pandas as pd
from opencage.geocoder import OpenCageGeocoder
from datetime import datetime
from event import Event
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf

# ConfigParser
config = configparser.ConfigParser()
config.read('config/meetup.cfg')


# Environ variables for API KEY
os.environ['MEETUP_API_KEY'] = config['MEETUP']['API_KEY']
os.environ['OPENCAGE_KEY'] = config['OPENCAGE']['KEY']

# Initialize OpenCage Geocode
geocode = OpenCageGeocoder(os.environ['OPENCAGE_KEY'])


def spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

    return spark


def request_event(topic, country=None, url_path):
    '''
    Function that make a request to meetup api and return a json with all events availables
    filtered by topic and country

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
    data = json.loads(results)
    return data['results']


def build_dataframe(spark, data):
    '''
    Build a SparkDataframe based on json response.
    Args:
        Spark (object): Spark Session
        data (json array): Event data from meetup api

    '''

    # Columns to build data frame
    columns = ['id', 'date', 'year', 'month', 'day', 'country', 'city', 'state,', 'address',
               'meetup_name', 'meetup_group_name', 'description', 'event_url', 'yes_rsvp_count', 'status']
    id, date, year, month, day, country, city, state, address, meetup_name, meetup_group_name, description, event_url, yes_rsvp_count, status = ([
    ] for i in range(15))

    # Iterate over events
    for label in data:
        date_event = datetime.fromtimestamp(label['time'] / 1000.0)

        id.append(label['id'])
        date.append(date_event)
        year.append(date_event.year)
        month.append(date_event.month)
        day.append(date_event.year)

        if label.get('venue'):
            country.append(label['venue'].get('country'))
            city.append(label['venue'].get('city'))
            state.append(label['venue'].get('state'))
            address.append(label['venue'].get('address_1'))
        else:
            location_json = geocode.reverse_geocode(
                label['group'].get('group_lat'), label['group'].get('group_lon'))
            country.append(location_json[0]['components'].get('country_code'))
            city.append(location_json[0]['components'].get('city'))
            state.append(location_json[0]['components'].get('state'))
            address.append(location_json[0].get('formatted'))

        meetup_name.append(label.get('name'))
        meetup_group_name.append(label['group'].get('name'))
        description.append(label.get('description'))
        event_url.append(label['event_url'])
        yes_rsvp_count.append(label.get('yes_rsvp_count'))
        status.append(label.get('status'))

    # Schema Structure
    schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("date", T.TimestampType(), True),
        T.StructField("year", T.IntegerType(), True),
        T.StructField("month", T.IntegerType(), True),
        T.StructField("day", T.IntegerType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("address", T.StringType(), True),
        T.StructField("meetup_name", T.StringType(), True),
        T.StructField("meetup_group_name", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("event_url", T.StringType(), True),
        T.StructField("yes_rsvp_count", T.IntegerType(), True),
        T.StructField("status", T.StringType(), True)
    ])

    # Create pandas DataFrame using numpy.transpose() method
    df_pandas = pd.DataFrame(np.transpose([id, date, year, month, day, country, city, state, address,
                                           meetup_name, meetup_group_name, description, event_url, yes_rsvp_count, status]), columns=columns)
    # Create Spark DataFrame
    df = spark.createDataFrame(df_pandas, schema=schema)

    return df


### DATA WRANGLING ###

# Upper lambda functions
upper_udf = F.udf(lambda x: x.upper())

# Remove tags from description column
@udf
def remove_tags_udf(text):
    import re
    TAG_RE = re.compile(r'<[^>]+>')
    return TAG_RE.sub('', text)


# Apply udf functions
df = df.withColumn('country', upper_udf(df.country))
df = df.withColumn('description', remove_tags_udf(df.description))


if __name__ == '__main__':

    # Create Spark Session
    spark = spark_session()
    # url meetup request
    url_meetup_api = "https://api.meetup.com/2/open_events"
    # get list of events by topic and country
    data = request_event(topic="Python", country=None, url_meetup_api)
    # Build dataframe
    df = build_dataframe(spark, data)
