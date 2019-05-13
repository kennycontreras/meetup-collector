import pandas as pd
import numpy as np


class Zipcode():

    def __init__(self, csv_path):
        self.csv_path = csv_path

    def build_df(self, *args, **kwargs):

        df = pd.read_csv(self.csv_path, delimiter=';')
        return df

    def create_list(self,
                    df,
                    *args,
                    **kwargs):

        list = df[['Zip', 'City', 'State']][df.State == 'NY'].drop_duplicates().values.tolist()
        return list
