

class Zipcode():

    def __init__(self, spark, csv_path):
        self.spark = spark
        self.csv_path = csv_path

    def zipcode_df(self, *args, **kwargs):

        df = self.spark.read.format('csv').option('header', 'true').load(self.csv_path)
        return df.count()
