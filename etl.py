"""etl.py
"""
import configparser
import pandas as pd
from ds import fact_evaluation7 as enade7
from ds import dim_groups

CONFIG_FILE = 'config.ini'

if __name__ == '__main__':

    # Read configuration File
    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        print('{}\'s config file read!'.format(config['DS']['NAME']))
    except:
        print('Unable to read config file ("{}")'.format(CONFIG_FILE))
        exit(-1)

    ###################
    ## Process CSV Files

    # dim_groups
    DF_GROUPS = dim_groups.extract(
        config['DS']['PATH'] + config['DS']['DIM_GROUP_FILE'])
    
    DF_AREA = dim_groups.extract(
        config['DS']['PATH'] + config['DS']['DIM_AREA_FILE'])
    
    # fact_evaluation7
    ds = pd.DataFrame()
    for src_file in config['DS']['FILES'].split(','):
        data_src = config['DS']['PATH'] + src_file

        # Extract
        print('{}. EXTRACT. Reading file. '.format(data_src), end='')
        if data_src.split('/')[-1] in ['ENADE_2016.csv.gz', 'ENADE_2017.csv.gz']:
            df = enade7.extract(data_src, decimal=',')
        else:
            df = enade7.extract(data_src, decimal='.')
        print('Done! {}'.format(df.shape))

        # Transform
        print('{}. TRANSFORM. Processing. '.format(data_src), end='')
        df = enade7.transform(df, DF_GROUPS, DF_AREA)
        print('Done! {}'.format(df.shape))

        # Append new df
        ds = ds.append(df, ignore_index=True)

    # Load Dataset
    enade7.load(ds,
                fname=config['DS']['DATASET_FILE'],
                ftype=config['DS']['DATASET_FTYPE'])
    print('{}. Dataset written! {}'.format(config['DS']['DATASET_FILE'],
                                           ds.shape))
