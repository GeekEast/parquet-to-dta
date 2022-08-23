from os import listdir
import pandas as pd

def convert_input_path_to_output_path(input_path: str, input_path_prefix:str, output_path_prefix: str):
    return input_path.split(".parquet")[0].replace(input_path_prefix, output_path_prefix)

def get_year_from_input_path(input_path: str):
    return input_path.split("public_scrape_kappas_")[1].split(".")[0]

def transform_parquet_to_dta(parquet_input_path):
    dta_output_path = convert_input_path_to_output_path(parquet_input_path, "data", "output")
    current_file_year = get_year_from_input_path(parquet_input_path)
    print(f"====== start {current_file_year} data transformation =======")


    print(f"====== read parquet from {parquet_input_path} starts")
    df_from_parquet = pd.read_parquet(parquet_input_path)

    print(f"====== separate {parquet_input_path} to quarterly dataframe starts")
    first_quarter  = df_from_parquet[df_from_parquet["quarter"] == f"{current_file_year}-03-31"]
    second_quarter = df_from_parquet[df_from_parquet["quarter"] == f"{current_file_year}-06-30"]
    third_quarter  = df_from_parquet[df_from_parquet["quarter"] == f"{current_file_year}-09-30"]
    fourth_quarter = df_from_parquet[df_from_parquet["quarter"] == f"{current_file_year}-12-31"]

    print(f"====== transform {parquet_input_path} to quarterly dta files starts")
    first_quarter.to_stata(f"{dta_output_path}_03_31.dta")
    second_quarter.to_stata(f"{dta_output_path}_06_30.dta")
    third_quarter.to_stata(f"{dta_output_path}_09_30.dta")
    fourth_quarter.to_stata(f"{dta_output_path}_12_31.dta")

    print(f"====== finish {current_file_year} data transformation =======")

def main():
    # get all raw data file paths
    raw_data_file_names = [f'./data/{filename}' for filename in listdir("./data/")]
    raw_data_file_names.sort()

    for raw_data_file_path in raw_data_file_names:
        transform_parquet_to_dta(raw_data_file_path)
    

if __name__ == "__main__":
    main()