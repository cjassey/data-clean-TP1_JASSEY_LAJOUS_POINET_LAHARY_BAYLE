import os
import requests
import numpy as np
import pandas as pd

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


def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
    df = pd.read_csv(
        data_fname,
        ...
        )
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""
    ...
    return df

def sanitize_data_cp(df:pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(
        data={
            'com_cp' : [
                    34000,
                    0,
                    0,
                    0,
                    34172,
                    0,
                    34267,
                    34070,
                    0,
                    34090,
                    34000,
                    0,
                    34000,
                    34000,
                    34000,
                    34000,
                    34000,
                    34070,
                    34000,
                    34000,
                    34000,
                    0,
                    34000,
                    0,
                    34000,
                    pd.Na,
                    34000,
                    34070,
                    34000,
                    34000,
                    34000,
                    34000,
                    34000,
                    34070,
                    34000,
                    34070,
                    34000,
                    34000,
                    34000,
                    34000,
                    34000,
                    0,
                    34000,
                    34070,
                    34080,
                    34000,
                    34070,
                    34000,
                    34070,
                    34070,
                    34000,
                    34000,
                    34080,
                    34080,
                    34070,
                    34000,
                    34000,
                    0,
                    0,
                    0,
                    0,
                    34080,
                    34080,
                    34000,
                    34080,
                    34080,
                    34070,
                    34080,
                    34000,
                    0,
                    0,
                    34090,
                    34070,
                    34080,
                    34000,
                    34000,
                    34000,
                    34000,
                    34090,
                    34000,
                    34000,
                    34000,
                    34070,
                    pd.Na,
                    34000,
                    34080,
                    34000,
                    34000,
                    0,
                    34000,
                    34080,
                    pd.Na,
                    34000,
                    34000,
                    pd.Na,
                    pd.Na,
                    pd.Na,
                    34080,
                    pd.Na,
                    pd.Na,
                    34000,
                    34000,
                    34000,
                    34000,
                    34080,
                    0,
                    34090,
                    34090,
                    34000,
                    34070,
                    34000,
                    34000,
                    34000,
                    34070,
                    34070,
                    34000,
                    34080,
                    34000,
                    34080,
                    34070,
                    34080,
                    0,
                    pd.Na,
                    34070,
                    34000,
                    34080,
                    34070,
                    34070,
                    34070,
                    34080,
                    34000,
                    34000,
                    34000,
                    34070,
                    34070,
                    0,
                    0,
                    pd.Na,
                    34000,
                    34000,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    pd.Na,
                    0,
                    0,
                    0,
                    pd.Na,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    34070,
                    34070,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    34263,
                    34000,
                    34000,
                    34070,
                    34000,
                    0,
                    34090,
                    34090,
                    34080,
                    34000,
                    34000,
                    34070
                ]
            
        }
        dtype={
            'com_cp'='int'
        }
    )
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