from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf


class Zipcode():

    def __init__(self, spark, csv_path):
        self.spark = spark
        self.csv_path = csv_path

    def build_df(self, *args, **kwargs):

        df = self.spark.read.format('csv').option('header', 'true').load(self.csv_path)
        return df
