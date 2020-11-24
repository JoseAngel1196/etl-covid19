import pandas as pd

def toDatetime(key, data):
    return pd.to_datetime(data[key])
