'''
@Author: Élaine Soucy
@Version : Juillet 2023

Ce script permet de faire l'insertion des données des .csv exportés du OneDrive des modules d'Hugo dans la base de données.
Si un ou des fichiers .csv se trouvent dans les sous-dossiers du dossier D:\SFTPRoot\ModuleAcquisition, alors le script s'occupera de 
lire les fichiers et d'insérer les données dans la base de données. Après la lecture et l'insertion des données, 
les fichiers seront supprimés du dossier D:\SFTPRoot\ModuleAcquisition.

Il est possible de rouler directement ce fichier pour insérer immédiatement les données dans la base de données si 
des fichiers de données sont disponibles dans les sous-dossiers du dossier D:\SFTPRoot\ModuleAcquisition.
Autrement, une tâche planifiée s'exécutant régulièrement permet d'exécuter ce script.

S'il y a un problème lors de l'exécution de ce script, un fichier log est créé. Ce fichier porte le nom 
errorInsertionModuleAcquisition.log et se trouve dans le même dossier que ce fichier. 

CONSIDÉRATIONS IMPORTANTES : Les noms de sous-dossiers de D:\SFTPRoot\ModuleAcquisition sont utilisés pour créer 
les bucket dans la base de données. Il est donc important de donner un nom significatif. Par exemple, pour le CETAB,
on voudra créer un dossier nommé CETAB dans le dossier D:\SFTPRoot\ModuleAcquisition.

Ce script prend en compte que les données de temps sont dans un fuseau UTC+0. 
'''  

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import csv
from os import listdir, path, walk, remove
import re
import logging
import datetime as dt
import sys

'''

'''
def is_float(valeur):
    try:
        float(valeur)
        return True
    except ValueError:
        return False    

if __name__ == '__main__':
    try:
        client = InfluxDBClient.from_config_file("config.ini")
        org_id = client.org
        api_write = client.write_api(write_options=SYNCHRONOUS)
        log_filename = 'errorInsertionModuleAcquisition.log'
            
        # Fichier qui contient le dossier ayant tous les sous-dossiers pour toutes les locations
        path_to_data = "D:\\SFTPRoot\\ModuleAcquisition"
        measurement = 'metrics'
        pattern_node = "(node_[0-9].)"
        
        # Trouver les buckets présents dans la base de donnée
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        buckets_name_list = []
        for b in buckets:
            buckets_name_list.append(b.name)
        
        for root_dir, sub_dir_list, files in walk(path_to_data):
            
            for sub_dir in sub_dir_list:
                
                """
                Créer le bucket si pas déjà existant
                """
                bucket_name = sub_dir
                
                if not bucket_name in buckets_name_list:
                    buckets_api.create_bucket(bucket_name=bucket_name, org_id=org_id)
                    # On ajoute à la liste plutôt que d'aller request les buckets à chaque fois (optimisation code)
                    buckets_name_list.append(bucket_name)
                
                csv_reader = None
                # On obtient la liste de tous les fichiers .csv disponibles dans le sous-répertoire
                csv_files_list = [csv_file for csv_file in listdir(path.join(root_dir, sub_dir)) if csv_file.endswith('.csv')]
                
                # Pour chaque fichier dans le dossier
                for csv_file in csv_files_list:
                    # Ouverture du fichier en mode lecture seule
                    namefile = root_dir + "\\" + sub_dir + "\\" + csv_file
                    with open(namefile, 'r') as f:   
                        csv_reader = list(csv.DictReader(f))
                    
                    for row in csv_reader:
                        unix_time = None
                        field_name = None
                        for field, value in row.items():
                            if field is not None :
                                value = value.lstrip().rstrip()
                                if value != 'NotFound' :
                                    field = field.lower().lstrip().rstrip()
                                    field = re.sub(" ", "_", field)

                                    if re.search(pattern_node, field) is not None:
                                        if (not is_float(value)):
                                            continue

                                        split_column = re.split(pattern_node, field)
                                        node_name = split_column[1].replace(".", "")
                                        field_name = split_column[-1]
                                        
                                        if unix_time is None:
                                            raise Exception(f"Erreur avec le unix time du fichier {csv_file}. Le programme se termine ... ")

                                        # On crée la structure de la donnée
                                        dict_structure = {
                                            "measurement": measurement,
                                            "tags": {"node_number": node_name},
                                            "fields": {field_name : float(value)},
                                            "time": unix_time
                                        }
                                        
                                        # On crée le point de donnée et on l'insère dans la base de donnée
                                        point = Point.from_dict(dict_structure, WritePrecision.S)
                                        api_write.write(bucket=bucket_name, record=point) 
                                        
                                    elif field =="unix_time" :
                                        # Unix time en UTC+0
                                        unix_time = int(value)
                    
                    # On supprime le fichier lu
                    remove(namefile)
                    
    except Exception as e:
        time = dt.datetime.now()
        logging.basicConfig(filename=log_filename, filemode='a', format='%(levelname)s - %(message)s')
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{time} - ligne {exc_tb.tb_lineno} - Type d'exception : {exc_type} \n{repr(e)}\n********************")
    # Ce bloc s'exécute peu importe qu'il y ait eu une exception ou non
    finally:
        client.close()
    
    

    

    
    
