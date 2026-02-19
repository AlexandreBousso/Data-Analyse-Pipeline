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

def load_database(source, api_key=None): #Je rassemble mes fonctions load_path et load_API dans une seule fonction afin de pouvoir automatiser plus simplement mon pipeline
    if source.startswith(("http://", "https://")):
        if api_key is None:
            print("Clé API requise")
            return(None)
        headers = {"Authorization":f'Bearer {api_key}'}
        try:
            response = requests.get(source, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json() 
                return pd.DataFrame(data)
            else:
                print(f"Erreur lors de la requête API, Code d'erreur : {response.status_code}") 
                return None
        except Exception as e:
            print(f"Une erreur s'est produite lors de la requête API: {e}")
            return None
    else:
        if not os.path.exists(source):
            print(f"Le fichier '{source}' n'existe pas.")
            return None
        extension = os.path.splitext(source)[1].lower() #permet d'extraire l'extension du fichier et mettre en minuscule. 
        try:
            if extension ==".csv":
                return pd.read_csv(source, sep=",")
            elif extension in [".xls", ".xlsx"]:
                return pd.read_excel(source)
            else:
                print("Format de fichier incompatible")
                return(None)
        except Exception as e:
            print(f"Une erreur s'est produite lors du chargement du fichier : {e}")
            return None

    


def df_info(df):
    print("Aperçu des données :")
    print(df.head())
    print("\n Informations sur les données :")
    print(df.info())
    return(df)


def check_missing(df):
    print("Analyse des valeurs manquantes :")
    missing = df.isnull().sum()
    summary = missing.reset_index()
    summary.columns = ["Colonne", "Nb manquants"]
    manquant= summary[summary['Nb manquants'] > 0]
    missing_percent = 100 * df.isnull().sum() / len(df)
    if manquant.empty:
        print("Aucune valeur manquante détectée.")
    else:
        print("Valeurs manquantes :")
        print(manquant)
        print(missing_percent)
    
    
    return(df)
   

def df_drop_NAN(df, subset=None): #Fonction qui permet de supprimer les lignes contenants des valeurs manquantes, subset=["colonne", "colonne2", etc], si subset est None (juste appeler df_drop_NAN(df) alors toutes les colonnes seront prises) 
    choix = input("Voulez-vous supprimer les lignes avec des valeurs manquantes ? (o/n) : ").lower()
    if choix == 'o':
        print("Suppression en cours...")
        return df.dropna().copy()
    else:
        print("Aucune suppression effectuée.")
        return(df)

def str_replace_values(df, column, old_val,  new_val):  #Fonction qui permet de remplacer une valeur string dans une colonne spécifique
    df[column]= df[column].str.replace(pat = old_val, repl= new_val, regex=False)
    return df

def replace_mapping(df, column, mapping):  #Fonction qui permet de remplacer les valeurs d'une colonne à l'aide d'un dictionnaire de mapping, mapping = {"old_value1:"new value1, ... "old_valueN:"new_valueN"}
    df=df.copy()
    df[column] = df[column].map(mapping)
    return df


#Fonction combo de mapping et replace values en fonction des besoins dans un soucis de faire un pipeline automatisé
#keep_others=True pour utiliser .replace 
#keep_others=False pour utiliser .map
def transform_value(df, column, mapping, keep_others=True):
    df=df.copy()
    if keep_others:
        print(f"Remplacement partiel dans {column} avec .replace")
        df[column] = df[column].replace(mapping)
    else:
        print(f"Remplacement total dans {column} avec .map")
        df[column] = df[column].map(mapping)
    return df

def check_missing_after_mapping(df, column): #fonction qui vérifie s'il y'a des valeurs manquantes après le mapping, à utiliser juste après (replace_mapping)
    missing = df[column].isna().sum()
    if missing > 0:
        print(f"⚠️ {missing} valeurs manquantes dans {column}")
    return(df)

def column_rename(df,old_name, new_name):
    df=df.copy()
    return df.rename(columns={old_name: new_name})

def filter_rows(df, conditions):        #Avec conditions un dictionnaire du type {column1:value1, etc}
    df = df.copy()
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df

def convert_dtypes(df, dtype_map): #dtype map un dictionnaire tel que {"column1":dtype1, "column2"::dtype2 etc} avec dtype, int, float, str & datetime
    df= df.copy()
    for column, dtype in dtype_map.items():
        try:
            if dtype == "datetime":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            else:
                df[column]= df[column].astype(dtype)
            print(f'Conversion de {column} en {dtype} réussie"')
        except Exception as e:
            print(f"Erreur lors de la conversion de {column} en {dtype} : {e}")
    return df

def aggregate_mean (df, columns, conditions):  #Fonction qui permet d'agréger les données en calculant la moyenne des values de "columns" après avoir filtré les donnés selons "conditions" de type conditions ={column1=value1, etc}
    df= df.copy()
    for column, value in conditions.items():
        df = df[df[column] == value]
    return df.groupby(columns).mean().reset_index()

#Fonction qui permet d'aréger les donnés à l'aide d'un filtre "conditions" tel que conditions ={column1=value1, etc}} 
#grpby_columns : liste des colonnes sur lesquelles on veut grouper sous la forme ["colonne1, "colonne2," etc]
#agg_logic : dictionnaire tel que {"colonne1":"mean", "ventes":"sum", etc...}

def aggregate (df, grpby_columns, agg_logic, conditions=None):  
    df= df.copy()
    if conditions is not None:
        for column, value in conditions.items():
            df = df[df[column] == value]
    return df.groupby(grpby_columns).agg(agg_logic).reset_index()

def saving_file(df, path, format): #Fonction qui permet de save un data frame au format "csv" ou "excel"
    if format =="csv":
        df.to_csv(f"{path}.csv", index=False)
    elif format in ["xls", "xlsx"]:
        df.to_excel(f"{path}.xlsx", index=False)
    else:
        print("Format de fichier incompatible, veuillez choisir entre 'csv' ou 'excel'")
    return df

#Assemblage sous forme de pipelines TLDR des fonctions précédentes
#1er pipeline : Chargement de la base de données et affichage des informations via df_info et check_missing puis supressions des lignes manquantes avec df_drop_NAN(avec demande utilisateur)
#Dans l'idée j'aimerai ajouter un pipeline intermédiaire pour filtrer et renommer les colonnes avant de faire le mapping mais pas une priorité actuelle, je vais me concentrer sur les opérations clés pour l'instant
#2nd pipeline : Mapping ou remplacement de valeurs dans une colonne en fonction de True(replace) ou False(map) et vérification des valeurs manquantes après mapping
#3éme pipeline: aggrégation des données à l'aide de conditions de filtrage et sauvegarde du résuldat dans un fichier csv ou excel
def pipeline_load_and_info(source, api_key=None):
    df=load_database(source, api_key)
    if df is None:
        print("Echec du chargement de la base de données")
        return(None)
    return (df.pipe(df_info).pipe(check_missing).pipe(df_drop_NAN))
def pipeline_mapping_and_check(df, column, mapping):
    return(df.pipe(transform_value, column=column, mapping=mapping, keep_others=True).pipe(check_missing_after_mapping, column=column))
def pipeline_powerbirdy(df, grpby_columns, agg_logic, conditions=None, path=None, format=None):
    return(df.pipe(aggregate, grpby_columns=grpby_columns, agg_logic=agg_logic, conditions=conditions).pipe(saving_file, path=path, format=format))



def create_test_data():
    data = {
        'Date': ['2023-01-01', '2023-01-02', 'invalide', '2023-01-04'],
        'Ville': ['pariis', 'lyon', 'marseille', 'lyon'], # Erreur "pariis"
        'Ventes': ['100', '200', '150', '300'], # En texte au lieu de nombre
        'Statut': ['V', 'V', 'A', 'V'] # Codes à mapper : V=Validé, A=Annulé
    }
    df_test = pd.DataFrame(data)
    df_test.to_csv("test_brut.csv", index=False)
    print("✅ Fichier 'test_brut.csv' créé pour l'exercice.")



mapping_ville = {"pariis": "paris"}
mapping_statut = {"V":"Validé", "A":"Annulé"}
data_type={"Date":"datetime", "Ventes":"float", "Ville":"str", "Statut":"str"}
agg_logic = {"Ventes":"sum"}

def run_full_test():
    print("--- DÉMARRAGE DU TEST ---")
    df_resultat= (load_database("test_brut.csv").pipe(df_info).pipe(check_missing).pipe(transform_value, column="Ville", mapping=mapping_ville, keep_others=True).pipe(transform_value, column="Statut",
     mapping=mapping_statut, keep_others=False).pipe(check_missing_after_mapping, column="Statut").pipe(convert_dtypes, dtype_map=data_type).pipe(df_drop_NAN).pipe(aggregate, grpby_columns=["Ville"], agg_logic=agg_logic).pipe(saving_file, path="export_power_bi", format="csv"))

    print("---Test terminé---")
    return df_resultat
if __name__=="__main__":
    create_test_data()
    resultat_final = run_full_test()
    print(resultat_final)