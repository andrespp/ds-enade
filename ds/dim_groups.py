"""dim_groups.py

    Grupo de enquadramento do curso no Enade

"""
import pandas as pd

def extract(data_src, gzip=False):
    """Extract data from source

    Parameters
    ----------
        data_src : string
            Path to CSV file

        gzip : boolean, default False
            Whether CSV file is compressed or not.

    Returns
    -------
        data : Pandas DataFrame
            Extracted Data
    """

    if gzip:
        compress = 'gzip'
    compress = 'infer'

    return pd.read_csv(data_src, compression=compress)

def transform(data):
    """Transform data

    Parameters
    ----------
        data | Pandas DataFrame

    Returns
    -------
        tdata : Pandas DataFrame
            Tranformed data
    """
    return data

def load(data, out_file, truncate=False):
    """Load data into the Parquet File

    Parameters
    ----------
        data : Pandas DataFrame
            Data to be written

        out_file : string
            Pathname for output file

        truncate | boolean, default False
            If true, truncate file before loading data
    """

    tmp = data, out_file, truncate

    return True

