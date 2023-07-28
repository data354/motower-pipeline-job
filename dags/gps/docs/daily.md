## DAILY DAG (EXTRACT)

### PURPOSE
Ce workflow continent 6 tâches et est chargé en général d’extraire des daily data et les stocker dans des fichiers CSV dans Minio. 
La path de stockage dans MINIO est: bucket_name/folder/année/mois/jour.csv

Elle interagit donc avec les daily sources (postgreSQl et FTP) et MINIO. Elle s’execute quotidiennement à 5h30. Les configurations concernant les sources sont contenues dans des variables airflow.

### TASKS
- ingest_Taux_succes_2g: Cette tâche est chargée de recupérer les données du success rate pour la technologie 2G. La source de données est postgreSQL (Table: Taux_succes_2g). Le bucket Minio est success-rate et le folder 2g;

- ingest_Taux_succes_3g: Cette tâche est chargée de recupérer les données du success rate pour la technologie 3G. La source de données est postgreSQL (Table: Taux_succes_3g). Le bucket Minio est success-rate et le folder 3g;
- Ingest_faitalarme: Cette tâche quant à elle récupère les données liées à l’indisponibilité. La source de données est postgreSQL (Table: faitalarme); Le bucket Minio est indisponibilite et le folder indisponibilite;
- Ingest_hourly_datas_radio_prod: recupère les données concernant le trafic. La source de données est postgreSQL (Table: hourly_datas_radio_prod). Le bucket Minio est trafic et le folder trafic
- Ingest_caparc: Cette tâche recupère les données concernant le CA et le PARC via FTP. La particularité de cette tâche est qu’elle recupère les données de d-7. Le declenchement de cette tâche depend du sensor_ca. Sensor_ca est une tâche spéciale qui verifie la présence du fichier à recupérer quotidiennement. Dès que le sensor dectecte la présence dufichier, il passe à succès et la tâche est exécutée. Au bout de 5 jours, si le fichier n’est toujours pas fourni dans la source,le sensor echoue et la tâche send_email est exécutée. Le bucket Minio est caparc et le folder caparc;
Send_email: envoie un mail au responsable du fichier CA et PARC afin qu’il fournissent le fichier manquant
