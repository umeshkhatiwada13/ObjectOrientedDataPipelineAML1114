from data_pipeline import DataProcessor

# Student names and roll numbers:
# Umesh Khatiwada : c089395
# Prajina Rajkarnikar : CC0906293
# Bibek Siwakoti : C0898100
# Anshu Patel : C0894980
# Mohibkhan pathan : C0894975

def main():
    # Initialize DataProcessor with the config file
    dp = DataProcessor('config.json')

    # Read the data files
    dfs = {
        'chartevents': dp.read_data(dp.config['chartevents_path'], ['hadm_id', 'charttime', 'itemid', 'valuenum']).head(1000),
        'outputevents': dp.read_data(dp.config['outputevents_path'], ['hadm_id', 'charttime', 'itemid', 'value']).head(1000),
        'patients': dp.read_data(dp.config['patients_path'], ['subject_id', 'gender']).head(1000),
        'admissions': dp.read_data(dp.config['admissions_path'], ['hadm_id', 'admittime', 'subject_id']).head(1000)
    }

    # Pivot the dataframes
    dfs['chartevents'] = dp.pivot_data(dfs['chartevents'], ['hadm_id', 'charttime'], 'itemid', 'valuenum')
    dfs['outputevents'] = dp.pivot_data(dfs['outputevents'], ['hadm_id', 'charttime'], 'itemid', 'value')


    merged_df = dp.merge_data(dfs)

    # Analyze the merged dataframe
    analysis = dp.analyze_data(merged_df)

    # Impute the missing values
    imputed_df = dp.impute_missing_values(merged_df)

    # Save the transformed dataframe and analysis
    dp.save_transformed_data(imputed_df, analysis)


if __name__ == "__main__":
    main()