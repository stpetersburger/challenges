import pandas as pd
import json


def concatenate_dataframes(df1=pd.DataFrame(), df2=pd.DataFrame()):

    if df1.empty:
        df1 = df2
    else:
        df1 = pd.concat([df1, df2])

    return df1


def read_files(pth):
    if '.json' in pth:
        return pd.read_json(pth)
    elif '.csv' in pth:
        return pd.read_csv(pth)


def print_json(json_str, fname, pth=None):
    with open(f"{pth}/{fname}.json", "w") as f:
        f.write(json_str)
        f.close()