import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple
from pandas.api.types import is_numeric_dtype, is_categorical_dtype

class DataProcessor:

    def __init__(self, config_file):
        with open(config_file) as json_file:
            self.config = json.load(json_file)

    def read_data(self, path: str, cols: List[str]) -> pd.DataFrame:
        df = pd.read_csv(path, usecols=cols)
        return df

    def pivot_data(self, df: pd.DataFrame, index: List[str], columns: str, values: str) -> pd.DataFrame:
        df_pivot = df.pivot_table(index=index, columns=columns, values=values)
        return df_pivot


    def merge_data(self, dfs: Dict[str, pd.DataFrame]):
        default_df = pd.DataFrame([])
        # Merge the dataframes
        merge_keys = {
            "admissions": "hadm_id",
            "patients": "subject_id",
            "chartevents": "hadm_id",
            "outputevents": "hadm_id"
        }

        for name, foreign_key in merge_keys.items():
            foreign_key = foreign_key.lower()

            if default_df.empty:
                default_df = dfs[name]
                continue

            # Determine the appropriate 'right' key for merging
            right_key = 'hadm_id' if name in ['chartevents', 'outputevents'] else 'subject_id'
            default_df = pd.merge(default_df, dfs[name], left_on=foreign_key, right_on=right_key, how='left')

        return default_df

    def analyze_data(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        analysis = {}
        for col in df.columns:
            if is_numeric_dtype(df[col]):
                analysis[col] = {
                    'missing_ratio': df[col].isnull().mean(),
                    'min': df[col].min(),
                    'max': df[col].max()
                }
            if is_categorical_dtype(df[col]):
                analysis[col] = {
                    'missing_ratio': df[col].isnull().mean(),
                    'mode': df[col].mode()[0]
                }
        return analysis

    def impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if is_numeric_dtype(df[col]):
                median = df[col].median()
                df[col].fillna(median, inplace=True)
            if is_categorical_dtype(df[col]):
                mode = df[col].mode()[0]
                df[col].fillna(mode, inplace=True)
        return df

    # def save_transformed_data(self, df: pd.DataFrame, analysis: Dict[str, Dict[str, float]]) -> None:
    #     df.to_csv(self.config['result_path'] + self.config['result_csv'], index=False)
    #
    #     with open(self.config['result_path'] + self.config['result_analysis'], 'w') as f:
    #         f.write(json.dumps(analysis, indent=4))

    @staticmethod
    def numpy_int64_to_int(data):
        if isinstance(data, dict):
            return {k: DataProcessor.numpy_int64_to_int(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataProcessor.numpy_int64_to_int(element) for element in data]
        elif isinstance(data, np.int64):
            return int(data)
        else:
            return data

    def save_transformed_data(self, df: pd.DataFrame, analysis: Dict[str, Dict[str, float]]) -> None:
        df.to_csv(self.config['result_path'] + self.config['result_csv'], index=False)

        # Convert np.int64 to int for JSON serialization
        analysis = self.numpy_int64_to_int(analysis)

        with open(self.config['result_path'] + self.config['result_analysis'], 'w') as f:
            f.write(json.dumps(analysis, indent=4))
