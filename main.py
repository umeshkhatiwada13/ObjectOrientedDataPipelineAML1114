# This is a sample Python script.

# Student name and roll number

from pre_processing import DataReader

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("I am running")
    dataReader = DataReader.DataReader
    dataReader.read_file()
    dataReader.pivot_columns()
    # dataReader.merge_columns()
    dataReader.impute_missing_values()
    dataReader.compute_missing_data_stats()
