# Déploiement

## tests en local

+ BDD : 
    + Lancer la BDD : `./start_dev_instance.sh` dans `components/database`
    + Stop BDD : `docker-compose down`
+ Job creation :
    + Créer image docker job creation : `docker build --rm -t csi_job_creation -f components/job_creation/docker/Dockerfile .`
    + Lancer job creation :
```
docker run --rm -it \
  --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
  --env CSI_SCIHUB_ACCOUNT_PASSWORD=dummy_value \
  --env CSI_SIP_DATA_BUCKET=dummy_value \
  --network=host \
  --name csi_job_creation_c \
  csi_job_creation
```


## tests système sur env test DIAS

+ `terraform state list` + destroy + build pour relancer juste 1 service
+ `openstack console log show tf-titi |less` pour voir les logs
+ créer fiche nomad pour processing job (ex: `./build/instance_init_packages/nomad_server/src/envsubst/rlies1_processing.nomad`) + ajouter à `build/instance_init_packages/nomad_server/src/init_instance.sh`
+ pas oublier de deploy la pipeline sur gitlab (environ 10 minutes)
+ `source ~/save_cosims/csi_test-openrc.sh` (password dans fichier login pour `os-automation-test`)
+ dans build, changer l'id git des .nomad pour l'orchestrateur puis `./build.sh`
+ lancer le système : `./test_env_apply.sh module.core`
+ arrêter le système : `./test_env_destroy.sh module.core`
+ détruire le contenu de la bdd : `./test_env_destroy.sh module.cold.openstack_blockstorage_volume_v2.database`



Mise à jour les paramètres systèmes au lancement du système: `cosims/deployment/terraform/database_patch.sql`


## test worker sur DIAS

+ fichiers `run_test*.sh` à mettre dans `cosims/components/worker/src`
+ à appeler depuis la racine `cosims/` en passant en argument l'ID du job que tu veux processer (une fois que tu as des jobs dans ta bdd en local, prêts à être processés, grace aux services de creation et de configuration) :
    + `curl http://localhost:3000/NOM_DE_LA_TABLE` pour avoir les IDs de job

__PS : [IMPORTANT]__ en sortie de configuration, tes jobs sont dans le statut ready, or ton run_worker s'attend à les recevoir dans le statut queued, ce qui vas générer des erreurs en l'état !
Je te conseille donc soit d'éditer le module `job_configuration.py` pour faire passer les jobs en queued directement après le statut ready, ou d'éditer le module `run_worker_template.py` pour faire le changement de statut en queued juste avant qu'il essaye de le passer en started.

__PS2 :__ SI tu veux pouvoir débugger plus facilement sans te prendre la tête avec les problèmes de changement de statut, qui te pousseront à redémarrer ta bdd à chaque fois, dans le module run_worker_template.py, tu peux mettre des
```
try:
except:
    pass
```

Autour des lignes de changement de statut, pour éviter que les jobs ne finissent dans des statuts d'erreur, et ne soient plus acceptés.

__PS3 :__ Si tu veux faciliter le debuggage de ton run_worker, je te conseille de commenter le `try/except` autour de la ligne `run_worker_instance.run()`, pour éviter de catcher les erreurs automatiquement, et que tu aies la trace des exceptions levées.


