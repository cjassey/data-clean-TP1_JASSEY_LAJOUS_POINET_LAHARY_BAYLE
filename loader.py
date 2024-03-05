import os
import requests
import numpy as np
import pandas as pd
import re

DATA_PATH = 'data/MMM_MMM_DAE.csv'

def download_data(url, force_download=False, ):
    # Utility function to donwload data if it is not in disk
    data_path = os.path.join('data', os.path.basename(url.split('?')[0]))
    if not os.path.exists(data_path) or force_download:
        # ensure data dir is created
        os.makedirs('data', exist_ok=True)
        # request data from url
        response = requests.get(url, allow_redirects=True)
        # save file
        with open(data_path, "w") as f:
            # Note the content of the response is in binary form: 
            # it needs to be decoded.
            # The response object also contains info about the encoding format
            # which we use as argument for the decoding
            f.write(response.content.decode(response.apparent_encoding))

    return data_path


def load_formatted_data(data_frame:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    df = pd.read_csv(
        data_frame,
        sep=","
        )
    

    

    df = pd.read_csv(data_frame,
                     delimiter= ',',
                     dtype={'nom':str,'lat_coor1':float,'long_coor1':float,'adr_num':str,'adr_voie':str,'com_cp':str,'com_nom':str,'tel1':str,'freq_mnt':str,'dermnt':str},
                     na_values=['']
                       )
    df['lat_coor1'] = pd.to_numeric(df['lat_coor1'],errors='coerce')
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""

    df['expt_tel1'] = df['expt_tel1'].str.replace('+','')

    def format_tel(tel):
        return re.sub(r'^(\d{3}) (\d{2} \d{2} \d{2} \d{2})$', r'+\1 \2', tel)
    df['expt_tel1'] = df['expt_tel1'].apply(format_tel)

    df['expt_tel1'] = df['expt_tel1'].str.replace("+33","+33 ")

    return df


def sanitize_frequence(df:pd.DataFrame) ->pd.DataFrame:
    """One function to do the sanitizing of the frequence maintenance column"""
    for i in range(0,len(df['freq_mnt'])) :
        if not pd.isna(df['freq_mnt'][i]):
            df['freq_mnt'][i] = 'tous les ans'
    return df

def sanitize_cp(df:pd.DataFrame) -> pd.DataFrame:
    """One function to do the sanitizing of the postal code column"""
    for i in range(0, len(df['com_cp'])) :
        if df['com_cp'][i] == '0':
            df['com_cp'][i] = pd.NA
    return df

# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df['address']=df['adr_num']+" "+df['adr_voie']+" "+df['com_cp']+" "+df['com_nom']
    df.drop(['adr_num','adr_voie','com_cp','com_nom'])
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    load_clean_data(download_data())