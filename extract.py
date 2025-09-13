# csv - json veri okuması

import pandas as pd

def extract_data():
    """
    data/raw/sample_pm25.csv dosyasını okur ve pandas DataFrame döner.
    """
    df = pd.read_csv(
        "data/raw/sample_pm25.csv",
        parse_dates=["timestamp"],    #timestamp kolonunu datetime yap
        dtype={"city":"string","pm25": "float"}  #city metin, pm25 sayı
    )
    return df

#test amaçlı çalıştırılabilir

if __name__ == "__main__":
    df = extract_data()
    print(df.head())