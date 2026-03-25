import numpy as np

def qaqc_usgs_data(df, data_column_name):
    #replace invalid -999999 values with NA
    try:
        df[data_column_name] = df[data_column_name].replace(-999999, np.nan)
    except KeyError:
        print(f"Does not have column {data_column_name}")
    # add any additional QAQC steps needed
    return df

def chunk_data(data, size):
    for i in range(0, len(data), size):
        yield data[i: i+size]
