import pandas as pd
import os
import requests

def load_path (path):   
    if not os.path.exists(path):
        print("Le chemin spécifié n'existe pas.")
        return None
    extension = os.path.splitext(path)[1].lower() #permet d'extraire l'extension du fichier et mettre en minuscule. 
    try:
        if extension ==".csv":
            return pd.read_csv(path, sep=",")
        elif extension in [".xls", ".xlsx"]:
            return pd.read_excel(path)
        else:
            print("Format de fichier incompatible")
            return(None)
    except Exception as e:
        print(f"Une erreur s'est produite lors du chargement du fichier : {e}")
        return None

def load_API(url, API_KEY):
    headers = {"Authorization":f'Bearer {API_KEY}'}

    response = requests.get(url, headers=headers) #On lance la requête avec GET
    if response.status_code == 200:
        data = response.json() #On convertit la réponse en format JSON
        return pd.DataFrame(data) #Pandas parse le JSON et le convertit en dataframe
    else:
        print(f"Erreur lors de la requête API, Code d'erreur : {response.status_code}") 
        return None
def df_info(df):
    print("Aperçu des données :")
    print(df.head())
    print("\n Informations sur les données :")
    print(df.info())


def df_drop_NAN(df, subset=None): #Fonction qui permet de supprimer les lignes contenants des valeurs manquantes, subset=["colonne", "colonne2", etc], si subset est None (juste appeler df_drop_NAN(df) alors toutes les colonnes seront prises) 
    df= df.copy()
    df = df.dropna().copy()
    return df

def str_replace_values(df, column, old_val,  new_val):  #Fonction qui permet de remplacer une valeur string dans une colonne spécifique
    df[column]= df[column].str.replace(pat = old_val, repl= new_val, regex=False)
    return df

def replace_mapping(df, column, mapping):  #Fonction qui permet de remplacer les valeurs d'une colonne à l'aide d'un dictionnaire de mapping, mapping = {"old_value1:"new value1, ... "old_valueN:"new_valueN"}
    df=df.copy()
    df[column] = df[column].map(mapping)
    return df

def check_missing_after_mapping(df, column): #fonction qui vérifie s'il y'a des valeurs manquantes après le mapping, à utiliser juste après (replace_mapping)
    missing = df[column].isna().sum()
    if missing > 0:
        print(f"⚠️ {missing} valeurs manquantes dans {column}")

def column_rename(df,old_name, new_name):
    df=df.copy()
    return df.rename(columns={old_name: new_name})

def filter_rows(df, conditions):        #Avec conditions un dictionnaire du type {column1:value1, etc}
    df = df.copy()
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df

def aggregate_mean (df, columns, conditions):  #Fonction qui permet d'agréger les données en calculant la moyenne des values de "columns" après avoir filtré les donnés selons "conditions" de type conditions ={column1=value1, etc}
    df= df.copy()
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df.groupby(columns).mean().reset_index()


def aggregate (df, columns, conditions):  #Fonction qui permet d'aréger les donnés à l'aide d'un filtre "conditions"
    df= df.copy()
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df.groupby(columns).reset_index()