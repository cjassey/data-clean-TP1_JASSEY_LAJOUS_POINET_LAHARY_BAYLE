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
    # Lis le CSV et remplace toutes les strings égales à "" ou " " par des pd.NA
    df = pd.read_csv(data_frame,
                     delimiter= ',',
                     na_values=['', ' ']
                       )
    
    # Remplace les "-" par des pd.NA 
    df['lat_coor1'] = df['lat_coor1'].replace('-', pd.NA)
    df['long_coor1'] = df['long_coor1'].replace('-', pd.NA)

    # Convertis en float les valeurs de la colonne, si la valeur ne peut pas etre convertis => pd.NA
    df['lat_coor1'] = pd.to_numeric(df['lat_coor1'],errors='coerce')
    df['long_coor1'] = pd.to_numeric(df['long_coor1'],errors='coerce')

    # Remplace les pd.NA par des np.NaN 
    df['lat_coor1'] = df['lat_coor1'].replace('<NA>', np.NaN)
    df['long_coor1'] = df['long_coor1'].replace('<NA>', np.NaN)


    # Replace NaN values by NA values, since those values are not numerical they cannot be considered as NaN values.
    df[['nom','adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt']] = df[['nom','adr_num', 'adr_voie', 'com_cp', 'com_nom', 'tel1', 'freq_mnt']].replace(np.NaN, pd.NA)

    # Cas particulier, deux valeurs sont inversées dans le dans le csv de bas, la prof a décidé de directement les remplacer par des NA dès le formatage  => pd.NA
    df.loc[5, 'freq_mnt'] = pd.NA
    df.loc[5, 'dermnt'] = pd.NA

    df = df.astype({
    'nom': 'string',
    'adr_num': 'string',
    'adr_voie': 'string',
    'com_cp': 'string',
    'com_nom': 'string',
    'tel1': 'string',
    'freq_mnt': 'string',
    'dermnt': 'string',

})
    df['dermnt'] = pd.to_datetime(df['dermnt'], errors='coerce')

    for column in df.columns:
        print(f"{column}: {df[column].dtype}")

    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""
    sanitized_df = sanitize_tel_number(df)
    sanitized_df = sanitize_adr_num(sanitized_df)
    sanitized_df = sanitize_com_nom(sanitized_df)
    sanitized_df = sanitize_adr_voie(sanitized_df)
    sanitized_df = sanitize_cp(sanitized_df)
    sanitized_df = sanitize_frequence(sanitized_df)

    sanitized_df = sanitized_df.astype({
    'nom': 'string',
    'adr_num': 'string',
    'adr_voie': 'string',
    'com_cp': 'string',
    'com_nom': 'string',
    'tel1': 'string',
    'freq_mnt': 'string',
    'dermnt': 'string',

})
    sanitized_df['dermnt'] = pd.to_datetime(sanitized_df['dermnt'], errors='coerce')
    return sanitized_df

def sanitize_tel_number(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to fix the format of the phone numbers (+33 X XX XX XX XX)"""

    def format_tel(tel):

        if type(tel) != str :
            return pd.NA
        elif re.match(r'^06 \d{2} \d{2} \d{2} \d{2}$', tel):
            # Enlever le 0 au début et remplacer par "+33"
            return re.sub(r'^0', '+33', tel)
        elif re.match(r'^\d{3} \d{2} \d{2} \d{2} \d{2}$', tel):
            # Si le numéro est déjà au format "334 67 27 46 12"
            return '+{}'.format(tel)
        elif re.match(r'^\+33 \d{2} \d{2} \d{2} \d{2}$', tel):
            # Si le numéro est déjà au format "+334 67 40 04 44"
            return tel
        else:
            return pd.NA
        
    df['tel1'] = df['tel1'].str.replace('+','').str.replace("  "," ")

    df['tel1'] = df['tel1'].apply(format_tel)

    df['tel1'] = df['tel1'].str.replace("+33","+33 ")
    return df

def sanitize_adr_num(df:pd.DataFrame) -> pd.DataFrame:
    """One function to sanitize the address number column"""    
    def clean_address_number(address_num):
        # Si l'adresse est une chaîne vide ou nulle, laisser telle quelle
        if pd.isna(address_num) or address_num.strip() == "" or address_num == "-":
            return pd.NA
        
        # Si le numéro d'adresse contient un tiret
        if "-" in address_num:
            # Supprimer les espaces inutiles autour du tiret
            address_num = "-".join(part.strip() for part in address_num.split("-"))

        if any(word.isalpha() for word in address_num.split()) and "bis" not in address_num.lower():
        # Supprimer les mots sauf "bis"
            address_num = " ".join(word for word in address_num.split() if word.lower() == "bis" or not word.isalpha())

        return address_num
    
    df['adr_num'] = df['adr_num'].str.replace(",", "")
    df['adr_num'] = df['adr_num'].apply(clean_address_number)
    return df

import re
def sanitize_adr_voie(df: pd.DataFrame) -> pd.DataFrame:
    """One function to sanitize the address name column"""
    def clean_address(address):
        if type(address) != str or len(address.strip()) <= 1:
            return pd.NA
        
        # Supprimer "Montpellier"
        address = re.sub(r'\b(?:M|montpellier)\b', '', address, flags=re.IGNORECASE)

        address = re.sub(r'\d+', '', address)

        # Supprimer les espaces au début et à la fin
        address = address.strip()
        
        # Traitement spécial pour les cas de "rue" suivi de "de"
        address = address.replace("  "," ").replace("Rue ","rue ")
   
        address = re.sub(r'(avenue)(\s+)([^\s]+)', lambda match: match.group(1) + match.group(2) + match.group(3).capitalize(), address, flags=re.IGNORECASE)
        address = re.sub(r'(rue)(\s+)([^\s]+)', lambda match: match.group(1) + match.group(2) + match.group(3).capitalize(), address, flags=re.IGNORECASE)

        # Gérer l'exception pour "avenue du"
        address = re.sub(r'(avenue\s+du)(?=\s+[^\s])', lambda match: match.group(1).lower(), address, flags=re.IGNORECASE)
        address = re.sub(r'(rue\s+du)(?=\s+[^\s])', lambda match: match.group(1).lower(), address, flags=re.IGNORECASE)
        address = re.sub(r'(rue\s+de)(?=\s+[^\s])', lambda match: match.group(1).lower(), address, flags=re.IGNORECASE)

        if "rue" in address:
            words = address.split()
            for word in words:
                if "-" in word:
                    word_to_correct_idx = words.index(word)
                    splitted_name = word.split("-")
                    splitted_name = [name.capitalize() for name in splitted_name]
                    corrected_name = splitted_name[0]+"-"+splitted_name[1]
                    words[word_to_correct_idx] = corrected_name
            address = " ".join(words)            

        return address.strip()

    df['adr_voie'] = df['adr_voie'].str.replace(",", "")
    df['adr_voie'] = df['adr_voie'].apply(clean_address)
    
    return df

    

def sanitize_com_nom(df:pd.DataFrame) -> pd.DataFrame:
    """One function to do the sanitizing of the city name column"""
    for i in range(len(df['com_nom'])):
        if not pd.isna(df['com_nom'][i]):
            df.loc[i, 'com_nom'] = 'Montpellier'
    return df

def sanitize_frequence(df: pd.DataFrame) -> pd.DataFrame:
    """One function to do the sanitizing of the frequence maintenance column"""
    for i in range(0, len(df['freq_mnt'])):
        if not pd.isna(df['freq_mnt'][i]):
            # Convertir la chaîne en minuscules
            df.loc[i, 'freq_mnt'] = df['freq_mnt'][i].lower()
            # Remplacer "tout" par "tous" dans la chaîne
            df.loc[i, 'freq_mnt'] = df['freq_mnt'][i].replace('tout', 'tous')
            # Changer les valeurs en "tous les ans"
            df.loc[i, 'freq_mnt'] = 'tous les ans'
    return df



def sanitize_cp(df:pd.DataFrame) -> pd.DataFrame:
    """One function to do the sanitizing of the postal code column"""
    for i in range(0, len(df['com_cp'])) :
        if df['com_cp'][i] == '0':
            df.loc[i, 'com_cp'] = pd.NA
    return df


# Define a framing function
def frame_data(df: pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    # Concaténer les colonnes en tenant compte des valeurs vides
    df['address'] = df['adr_num'].fillna('') + " " + df['adr_voie'].fillna('') + " " + df['com_cp'].fillna('') + " " + df['com_nom'].fillna('')
    df['address'] = df['address'].str.strip()
    df['address'] = df['address'].str.replace(r'\s+', ' ', regex=True)
    # Vérifier si adr_voie est vide et remplacer l'adresse par pd.NA
    df.loc[df['adr_voie'].isna() | (df['adr_voie'] == ''), 'address'] = pd.NA
    # Supprimer les colonnes obsolètes
    df.drop(['adr_num', 'adr_voie', 'com_cp', 'com_nom'], axis=1, inplace=True)
    # Insérer la colonne "address" en deuxième position
    df.insert(1, 'address', df.pop('address'))

    df = df.astype({
    'nom': 'string',
    'address': 'string',
    'tel1': 'string',
    'freq_mnt': 'string',

})
    df['dermnt'] = pd.to_datetime(df['dermnt'], errors='coerce')
    # for column in df.columns:
    #     print(f"{column}: {df[column].dtype}")

    return df

# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    print(df)
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    df = load_clean_data()
