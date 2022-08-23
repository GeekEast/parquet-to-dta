## Parquet to Stata dta format

### Prerequisite
- conda: `>= 4.11.0`
- dependencies: `pip install pandas pyarrow fastparquet` 

### Run the scripts
- prepare the input data in `./data` folder
```sh
conda activate base
python transform_parquet_to_dta.py
```