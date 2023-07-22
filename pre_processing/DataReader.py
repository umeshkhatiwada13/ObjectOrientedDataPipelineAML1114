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
        print("Inside pivot columns ")
        df_chart_events = pd.read_csv(os.getcwd() + "\\Files\\input_files\\CHARTEVENTS.csv",
                                      usecols=["hadm_id", "charttime", "valuenum", "itemid"])
        print(df_chart_events.head())

        df_chart_events_pivoted = pd.pivot_table(data=df_chart_events, index=["hadm_id", "charttime"],
                                                 columns=["itemid"],
                                                 values="valuenum")

        print("CHARTEVENTS Data has been pivoted")
        print(df_chart_events_pivoted)

        df_chart_events_pivoted.to_csv(os.getcwd() + "\\Files\\input_files\\CHARTEVENTS_PIVOT.csv")

        with open(os.getcwd() + "\config.json", 'r') as file:
            print(os.getcwd())
            df_output_events = pd.read_csv(os.getcwd() + "\\Files\\input_files\\OUTPUTEVENTS.csv",
                                           usecols=["hadm_id", "charttime", "value", "itemid"])
            print(df_output_events.head())

            df_output_events_pivoted = pd.pivot_table(data=df_chart_events, index=["hadm_id", "charttime"],
                                                      columns=["itemid"],
                                                      values="value")

            print("OUTPUTEVENTS Data has been pivoted")

            print(df_output_events_pivoted)

            df_output_events_pivoted.to_csv(os.getcwd() + "\\Files\\input_files\\OUTPUTEVENTS_PIVOT.csv")

        @staticmethod
        def merge_columns():
            df_chart_events = pd.read_csv("\\Files\\input_files\\CHARTEVENTS.csv")
            df_output_events = pd.read_csv("\\Files\\input_files\\OUTPUTEVENTS.csv")
            df_patients = pd.read_csv("\\Files\\input_files\\PATIENTS.csv")
            df_admission = pd.read_csv("\\Files\\input_files\\PATIENTS.csv")
