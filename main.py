# This is a sample Python script.

from pre_processing import DataReader

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("I am running")
    # DataReader.read_file()
    dataReader = DataReader.DataReader
    dataReader.read_file()
    dataReader.pivot_columns()
