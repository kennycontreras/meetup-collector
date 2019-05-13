import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf


class Event():

    def __init__(self,
                 spark,
                 data,
                 geocode,
                 *args, **kwargs):
        self.spark = spark
        self.data = data
        self.geocode = geocode

    def build_dataframe(self, *args, **kwargs):
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
        for label in self.data:
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
                location_json = self.geocode.reverse_geocode(
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
        df = self.spark.createDataFrame(df_pandas, schema=schema)

        # Upper lambda functions
        upper_udf = udf(lambda x: x.upper())

        # Remove tags from description column
        @udf
        def remove_tags_udf(text):
            import re
            TAG_RE = re.compile(r'<[^>]+>')
            return TAG_RE.sub('', text)

        # # Apply udf functions
        df_upper = df.withColumn('country', upper_udf(df.country))
        df_tags = df_upper.withColumn('description', remove_tags_udf(df_upper.description))

        return df_tags

    def write_df(self, df):
        df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option(
            "database", "meetup").option("collection", "events").save()
