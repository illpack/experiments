__author__ = 'Fabio Galicia'

import pandas as pd
import numpy as np
try:
    from pandas.api.types import is_datetime64tz_dtype
except ImportError:
    # pandas < 0.19.2
    from pandas.core.common import is_datetime64tz_dtype
import dask.dataframe as dd

filename = 'https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv'

def main():
    print('Downloading to dask dataframe...')
    df = dd.read_csv(filename, assume_missing=True, blocksize=128e6)
    print('Download complete. Computing...')
    asr_tip = df.tip_amount.values.compute()
    print('Length is: ',  str(asr_tip.size))
    print('Average is',  str(asr_tip.mean()))

if __name__ == "__main__":
    main()
