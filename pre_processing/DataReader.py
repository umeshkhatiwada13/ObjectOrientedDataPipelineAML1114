import json
import os

import pandas as pd


class DataReader:
    # def __init__(self):
    #     self.filename = None

    @staticmethod
    def read_file():
        # json.load()
        print("I am about to read file")
        with open(os.getcwd() + "\config.json", 'r') as file:
            data = json.load(file).get('file_locations')
        table_data = []
        for key, value in data.items():
            print(os.getcwd() + '/input_files/' + value)
            df = pd.read_csv(os.getcwd() + "\\Files\\input_files\\" + value)
            table_data.append(df)
        return table_data

    @staticmethod
    def pivot_columns():
        data = DataReader.read_file()
        # prinx
        print(data)
        df = pd.read_csv(r"mimic-iii-clinical-database-demo-1.4/mimic-iii-clinical-database-demo-1.4/CHARTEVENTS.csv",
                         usecols=["hadm_id", "charttime", "valuenum", "itemid"])

        df_pivoted = pd.pivot_table(data=df, index=["hadm_id", "charttime"],
                                    columns=["itemid"],
                                    values="valuenum")

        df_pivoted.to_csv("CHARTTIME_PIVOT.csv")
