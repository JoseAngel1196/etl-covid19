import pandas as pd

def to_datetime(key, data):
    return pd.to_datetime(data[key])
