
def zipcode_dataframe(spark, csv_path):

    df = spark.read.format('csv').option('header', 'true').load(csv_path)
    return df.count()
