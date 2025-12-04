# hackathon-climat-donnees

## Projet


### Contexte

Ce projet est réalisé dans le cadre du hackathon [Le climat en données](https://guides.data.gouv.fr/guide-du-participant-au-hackathon-le-climat-en-donnees),
par l'équipe Eight'xtrêmes : Marion BASTIEN, Nathan BENGAOUER, Thomas GRANDJEAN, Samy KRAIEM, Ana LUTZKY, Arthur RUSSIAS, Sophie TALBOT, Louisa VIRET.

### License

GPL-3.0 license

### Problématique et proposition de valeur

A quelles conditions météorologiques extrêmes les sites SEVESO pourraient-ils être confrontés au cours du XXIe siècle ?

Calcul des températures maximales sur plusieurs périodes de retour pour les trois niveaux de réchauffement TRACC et la période historique. 

### Solution

La solution permet d’évaluer l’évolution de la période de retour des évènements avec le changement climatique sur les sites SEVESO.

L’ensemble des couples GCM/RCM issues des nouvelles données EURO-CORDEX fournissant les températures maximales journalières sont utilisés. Une approche par niveau de réchauffement (TRACC) est choisie et les périodes correspondantes sont étudiées pour en extraire les maximums annuels et les ajuster sur une loi GEV. Cela permettra d'estimer les températures maximales associées à différentes périodes de retour.

Une visualisation graphique est mise en place à l’échelle nationale avec la localisation de sites SEVESO. 

Pour chaque site plusieurs données seront disponibles : 

* Carte du site SEVESO sélectionné
* Lien vers la fiche géorisques du site 
* Graphique illustrant les températures max (historique, +2°C, +2,7°C et +4°C) en fonction de la période de retour

Lien vers la cartographie produite :

[![Copie d'écran](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/media/video.gif)](https://datawrapper.dwcdn.net/0qH9I/29/)

### Impact envisagé

Gestionnaires et personnel des sites SEVESO : adaptation des sites au changement climatique :

* en termes de processus industriel potentiellement impacté par la hausse des températures (seul le gestionnaire du site est à même d’évaluer cet impact)
* en termes de qualité de vie au travail pour les personnels et gestionnaires

Si extension du POC aux autres jeux de données (cf. retour sur les données) : collectivités locales, services de l’État et associations d’usagers sur l’impact en matière de santé-environnement.

### Sources de données

* données contextuelles sur les installations classées : https://www.georisques.gouv.fr/ (pipelines de données reproductibles dans le repository)
* données météofrance mises à disposition durant le hackathon

## Organisation du repo

* les données d'entrée météo doivent être placées dans le répertoire INPUT. Celles utilisées sont celles des coupes GCM/RCM issues de nouvelles données EURO-CORDEX. Durant le hackathon, ces données sont disponibles sur [ce stockage objet](https://console.object.files.data.gouv.fr/browser/meteofrance-drias/SocleM-Climat-2025%2FRCM%2FEURO-CORDEX%2FEUR-12%2F)
* constitution d'un dataset ICPE : [prep_datasets.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/prep_datasets.py). Le fichier peut être exécuté directement pour générer un dataset comprenant un certain nombre de filtres décrits dans le code : ce dataset peut tout à fait être remplacé par d'autres jeux de données selon la thématique choisie.
* traitement des données météo : [netcdf_processing.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/netcdf_processing.py). Ce fichier peut être exécuté directement pour traiter les données météo.
* exploration des données météo : [prototype_exploration.ipynb](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/prototype_exploration.ipynb). Ce notebook peut être utilisé pour explorer les données.
* jointure des datasets : [join_netcdf.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/join_netcdf.py). Ce fichier peut être utilisé pour apparier des datasets netcdf et un dataset spécifique.

## Usage

``` python

from glob import glob
from hackathon_climat_donnees import OUTPUT
from hackathon_climat_donnees.prep_datasets import prep_dataset_icpe
from hackathon_climat_donnees.netcdf_processing import process_netcdf_bunch
from hackathon_climat_donnees.join_netcdf_to_dataframe import all_scenarii


gdf = prep_dataset_icpe()
process_netcdf_bunch()
# Sélection (par exemple) des fichiers générés sur la température médiane (multi-modèles)
scenarii = list(glob(os.path.join(OUTPUT, "*_median.nc")))
df = all_scenarii(gdf, scenarii)

print(df.head())

>>                                                          2          5  \
>> code_aiot  scenario                                                     
>> 0003205293 tasmaxAdjust_RP_hist_ref_median.nc    36.008649  37.912610   
>>            tasmaxAdjust_RP_ssp3_+2.7C_median.nc  39.366658  41.113695   
>>            tasmaxAdjust_RP_ssp3_+2C_median.nc    38.138694  39.984992   
>>            tasmaxAdjust_RP_ssp3_+4C_median.nc    40.412740  42.235474   
>> 0003300469 tasmaxAdjust_RP_hist_ref_median.nc    34.235854  36.320575   
>> 
>>                                                         10         20  \
>> code_aiot  scenario                                                     
>> 0003205293 tasmaxAdjust_RP_hist_ref_median.nc    38.927130  39.756595   
>>            tasmaxAdjust_RP_ssp3_+2.7C_median.nc  42.110725  42.995803   
>>            tasmaxAdjust_RP_ssp3_+2C_median.nc    40.802209  41.716499   
>>            tasmaxAdjust_RP_ssp3_+4C_median.nc    43.127427  43.808692   
>> 0003300469 tasmaxAdjust_RP_hist_ref_median.nc    37.797054  39.340793   
>> 
>>                                                         50        100  
>> code_aiot  scenario                                                    
>> 0003205293 tasmaxAdjust_RP_hist_ref_median.nc    40.714980  41.258031  
>>            tasmaxAdjust_RP_ssp3_+2.7C_median.nc  44.042835  44.459542  
>>            tasmaxAdjust_RP_ssp3_+2C_median.nc    43.334877  44.362507  
>>            tasmaxAdjust_RP_ssp3_+4C_median.nc    44.496478  44.954355  
>> 0003300469 tasmaxAdjust_RP_hist_ref_median.nc    41.208434  42.706186  

```

## Retours consolidés sur les données exploitées

Autres problèmes rencontrés : 

* données “vent” : pour analyser l’impact des panaches de rejets atmosphériques des sites, la décomposition zonale et méridionale est au moins aussi intéressante que la force (impacts potentiels en terme de santé-environnement en environnement urbain). Une granularité plus fine que la moyenne journalière serait également utile.
* pour identifier les enjeux sur les sites industriels en termes de risques naturels dans le contexte climatique, d’autres datasets mériteraient d’être traités de concert, ils ne l’ont pas été faute de temps durant le hackathon :
    * https://sealevelrise.brgm.fr/ (risques côtiers, retrait du trait de côte)
    * Explore 2 (effets du changement climatique sur les cours d’eau : effets sur les volumes prélevés, sur la dilution des rejets, les inondations)
    * Simulations sur l’indicateur RGA pour le retrait gonflement des argiles par météofrance, le BRGM et la caisse centrale de réassurance
