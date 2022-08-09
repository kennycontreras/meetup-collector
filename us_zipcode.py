import pandas as pd


class Zipcode:

    def __init__(self, csv_path):
        self.csv_path = csv_path

    def build_df(self, *args, **kwargs):
        return pd.read_csv(self.csv_path, delimiter=';')

    @staticmethod
    def create_list(df, *args, **kwargs):
        return df[['Zip', 'City', 'State']][df.State == 'NY'].drop_duplicates().values.tolist()
