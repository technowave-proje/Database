# kolon temizliği ve kalite kuralları

import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame üzerinde kolon adlarını, formatlarını ve kalite kurallarını uygular.
    """

    # 1-Kolon adlarını standardize et
    df = df.rename(columns={
        "city": "city_name",
        "timestamp": "measurement_time",
        "pm25": "pm25"
    })

    # 2- city_name küçük harfe çevir, boşlukları temizle
    df["city_name"] = df["city_name"].str.strip().str.lower()

    # 3- quality_flag kolonunu oluştur
    df["quality_flag"] = 0

    #eksik değer -> flag = 1
    df.loc[df["pm25"].isna(), "quality_flag"] = 1

    #aykırı değer (örnek kural: pm25 > 500 anormal) -> flag = 2
    df.loc[df["pm25"] > 500, "quality_flag"] = 2

    return df

# test amaçlı
if __name__ == "__main__":
    import extract
    raw_df = extract.extract_data()
    clean_df = transform_data(raw_df)
    print(clean_df)