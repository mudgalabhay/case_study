import os

def data_loader(spark, file_path):
    """
    Read csv file and load into dataframe
    Parameters:
        - spark : spark instance
        - file_path : path to input csv file
    Return:
        - dataframe
    """
    dataframes = {}

    for file_name in os.listdir(file_path):

        if file_name.endswith(".csv"):
            
            key = file_name.split(".")[0]
            dataframes[key] = spark.read.csv(os.path.join(file_path, file_name), header=True, inferSchema=True)
    
    return dataframes


