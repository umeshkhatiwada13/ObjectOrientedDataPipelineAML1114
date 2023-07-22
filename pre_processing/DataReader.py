import json
import os
import pandas as pd
import numpy as np


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

        df_chart_events_pivoted.head(1000).to_csv(os.getcwd() + "\\Files\\pivoted_files\\CHARTEVENTS_PIVOT.csv")

        print(os.getcwd())
        df_output_events = pd.read_csv(os.getcwd() + "\\Files\\input_files\\OUTPUTEVENTS.csv",
                                       usecols=["hadm_id", "charttime", "value", "subject_id"])
        print("Output events head")
        print(df_output_events.head())

        df_output_events_pivoted = pd.pivot_table(data=df_output_events, index=["hadm_id", "charttime"],
                                                  columns=["subject_id"],
                                                  values="value")

        print("OUTPUTEVENTS Data has been pivoted")

        print(df_output_events_pivoted)

        df_output_events_pivoted.head(1000).to_csv(os.getcwd() + "\\Files\\pivoted_files\\OUTPUTEVENTS_PIVOT.csv")

    @staticmethod
    def merge_columns():
        df_output_events_pivoted = pd.read_csv(os.getcwd() + "\\Files\\pivoted_files\\OUTPUTEVENTS_PIVOT.csv")
        df_chart_events_pivoted = pd.read_csv(os.getcwd() + "\\Files\\pivoted_files\\CHARTEVENTS_PIVOT.csv")
        df_patients = pd.read_csv(os.getcwd() + "\\Files\\input_files\\PATIENTS.csv")
        df_admission = pd.read_csv(os.getcwd() + "\\Files\\input_files\\ADMISSIONS.csv")

        df_merge = pd.merge(left=df_chart_events_pivoted, right=df_admission, on=['hadm_id'])
        df_merge = pd.merge(left=df_merge, right=df_patients, on=['hadm_id'])
        df_merge = pd.merge(left=df_merge, right=df_patients, on=['hadm_id'])

        df_merge.to_csv(os.getcwd() + "\\Files\\merged_files\\MERGED.csv")

    @staticmethod
    def compute_missing_data_stats():
        # Initialize an empty dictionary to store the results
        result_dict = {}

        df = pd.read_csv(os.getcwd() + "\\Files\\input_files\\CHARTEVENTS.csv")
        # df = df.head(100)

        # Get the total number of rows in the DataFrame
        total_rows = df.shape[0]

        # Calculate the percentage of missing data in each column
        missing_percentages = df.isnull().sum() / total_rows * 100
        result_dict['missing_percentage'] = missing_percentages.to_dict()

        # Get statistics (min and max for numeric columns, mode for categorical columns)
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            result_dict[col] = {
                'min': df[col].min(),
                'max': df[col].max()
            }

        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_columns:
            result_dict[col] = {
                'mode': df[col].mode().values[0]
            }

        print("Missing Data Statistics:")
        # df.to_csv(os.getcwd() + "\\Files\\output_files\\data_statistics.csv")
        print(result_dict)
        return result_dict

    @staticmethod
    def impute_missing_values():
        df = pd.read_csv(os.getcwd() + "\\Files\\input_files\\CHARTEVENTS.csv")
        # df = df.head(100)
        # Impute missing values with the median for numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            median_value = df[col].median()
            df[col].fillna(median_value, inplace=True)

        # Impute missing values with the mode for categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_columns:
            mode_value = df[col].mode().values[0]
            df[col].fillna(mode_value, inplace=True)

        print("\nImputed DataFrame:")
        print(df)
        df.to_csv(os.getcwd() + "\\Files\\output_files\\data_statistics.csv")
