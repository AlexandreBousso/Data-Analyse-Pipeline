@echo off
echo ------------------------------------------
echo LANCEMENT DU PIPELINE ETL - Entreprise XXXXX
echo ------------------------------------------

:: Remplace par le chemin vers ton dossier de projet
cd /d "E:\Prog\Python\Data_Eng_Folder"

:: On lance le script avec l'exécutable Python
python Pipeline_data.py

echo ------------------------------------------
echo TRAITEMENT TERMINE.
echo ------------------------------------------
pause