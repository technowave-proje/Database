from extract import extract_data
from transform import transform_data
from load import load_data

def main():
    #extract
    df_raw = extract_data()

    #transform
    df_clean = transform_data(df_raw)

    #load
    load_data(df_clean)

if __name__=="__main__":
    main()