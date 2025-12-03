# hackathon-climat-donnees

## Projet


### Contexte

Ce projet est réalisé dans le cadre du hackathon [Le climat en données](https://guides.data.gouv.fr/guide-du-participant-au-hackathon-le-climat-en-donnees),
par l'équipe Eight'xtrêmes.

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

![./img/screenshot.png](https://datawrapper.dwcdn.net/0qH9I/29/)

### Impact envisagé

Gestionnaires et personnel des sites SEVESO : adaptation des sites au changement climatique :
en termes de processus industriel potentiellement impacté par la hausse des températures (seul le gestionnaire du site est à même d’évaluer cet impact)
en termes de qualité de vie au travail pour les personnels et gestionnaires

Si extension du POC aux autres jeux de données (cf. retour sur les données) : collectivités locales, services de l’État et associations d’usagers sur l’impact en matière de santé-environnement.

### Sources de données

* données contextuelles sur les installations classées : https://www.georisques.gouv.fr/ (pipelines de données reproductibles dans le repository)
* données météofrance mises à disposition durant le hackathon

## Organisation du repo

* constitution d'un dataset ICPE : [prep_datasets.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/prep_datasets.py)
* exploration des données météo : [prototype_exploration.ipynb](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/prototype_exploration.ipynb)
* traitement des données météo : [netcdf_processing.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/netcdf_processing.py)
* jointure des datasets : [join_netcdf.py](https://github.com/tgrandje/hackathon-climat-donnees/blob/main/src/hackathon_climat_donnees/join_netcdf.py)

