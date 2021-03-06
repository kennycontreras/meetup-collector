import os
import requests
import configparser
import json
import pymongo
from opencage.geocoder import OpenCageGeocode
from event import Event
from pyspark.sql import SparkSession
from us_zipcode import Zipcode

# ConfigParser
config = configparser.ConfigParser()
config.read('config/meetup.cfg')


# Environ variables for API KEY
os.environ['MEETUP_API_KEY'] = config['MEETUP']['API_KEY']
os.environ['OPENCAGE_KEY'] = config['OPENCAGE']['KEY']
os.environ['MONGO_PASS'] = config['MONGODB']['PASS']

# Initialize OpenCage Geocode
geocode = OpenCageGeocode(os.environ['OPENCAGE_KEY'])

mongo_conn_str = "mongodb://mongoadmin:{}@dev-mongo-shard-00-00-klryn.mongodb.net:27017,dev-mongo-shard-00-01-klryn.mongodb.net:27017,dev-mongo-shard-00-02-klryn.mongodb.net:27017/test?ssl=true&replicaSet=dev-mongo-shard-0&authSource=admin&retryWrites=true".format(
    os.environ['MONGO_PASS'])


def spark_session():
    spark = SparkSession \
        .builder \
        .appName("meetupcollections") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.0")\
        .config("spark.mongodb.input.uri", mongo_conn_str) \
        .config("spark.mongodb.output.uri", mongo_conn_str) \
        .getOrCreate()

    return spark


def request_event(topic, country, url_path, zipcode_list):
    '''
    Function that make a request to meetup api and return a json with all events availables
    filtered by topic and country

    Args:
        topic (string): Topic of the events
        country (string): Country of the events
        url_path (string): Url for meetup API https://www.meetup.com/es-ES/meetup_api/docs/
    '''

    data = []

    for zipcode, city, state in zipcode_list:
        default_args = dict(
            zip=zipcode,
            topic=topic,
            key=os.environ['MEETUP_API_KEY']
        )

        print(default_args)

        r = requests.get(url_path, params=default_args)
        results = r.text
        try:
            json_data = json.loads(results)
            data.append(json_data['results'])
        except:
            print('Error with file {}'.format(json_data))

    return data


if __name__ == '__main__':

    # Create Spark Session
    spark = spark_session()
    # url meetup request
    url_meetup_api = "https://api.meetup.com/2/open_events"
    csv_zipcode_path = ("../meetup-mongo/data/us-zip-code.csv")
    # Initialize zipcode class
    zipcode = Zipcode(csv_zipcode_path)
    # create pandas dataframe
    df_zipcode = zipcode.build_df()
    # Zipcode list with zipcode, city and state for request
    zipcode_list = zipcode.create_list(df_zipcode)
    # get list of events by topic, country, zipcode, state and city
    data = request_event(topic="Python", country="US",
                         url_path=url_meetup_api, zipcode_list=zipcode_list)
    print(data)
    # Build dataframe
    # event = Event(spark, data, geocode)
    # df_event = event.build_df()
    # print(df_event.count())
    # write data into mongodb cluster
    # write_data(df, spark)
