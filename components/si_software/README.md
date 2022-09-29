
# Doc

V3 S&I software

- sous-dossier src : programme run_snow_ice.py et modules associés
- sous-dossier exemples : exemple de fichiers de paramètres run_snow_ice.yaml / run_sow_ice_docker.yaml
- sous-dossier docker : contient make_docker_file.py qui permet de construire l'image docker

Pour construire l'image docker :
- télécharger l'archive qui contient MAJA,OTB,SNAP,LIS et ICE : sur google drive dans *2019-EEA-CoSIMS(Consortium)/5-SoftwareAndData/52-Software/cosims dependencies/docker_install_bundle.tar.gz*
- l'extraire dans un dossier $docker_install_bundle
- lancer la commande "./make_docker_file.py --csi_root_dir=$csi_root_dir $docker_install_bundle"
    => $csi_root_dir est le path du dossier (ou l'archive) contenant le répertoire GIT racine de CoSIMS : /my_dir/cosims
    => ajouter à cette commande "--squash" pour réduire la taille de l'image docker, et/ou "--clean" pour reset tout le docker (docker system purge -a)
    => NB: 2 images sont créées (1 avec OTB, SNAP ~1h, et une autre avec MAJA/LIS/ICE/csi_software < 1min)


Pour exécuter l'image docker :
- télécharger l'archive qui contient des fichiers d'entrée S2 pour le test : *CoSIMS(Magellium)/.../912-Travail/data/V3/test_bundle.tar.gz*
- l'extraire dans un dossier $test_bundle
- modifier le paramètre output_dir dans $test_bundle/run_sow_ice_docker.yaml ​: il doit correspondre à un espace en dehors du docker dans lequel le S&I software mets les sorties.
- créer un dossier $test_docker_out
- éditer et lancer la commande (présente dans si_software/exemples/run_docker) : 
"docker run --rm -it \
    -v /mnt/data_hdd/snow_and_ice/snow_and_ice_scheduler_full/test_bundle:/test_bundle -v /mnt/data_hdd/snow_and_ice/snow_and_ice_scheduler_full/test_docker:/test_docker_out \
    -v /mnt/data_hdd/docker_temp:/docker_temp \
    --env csi_si_software_parameter_file=test_bundle/run_snow_ice_docker.yaml \
    csi_si_software:1.0 $@
"

La partie de la commande "-v /mnt/data_hdd/docker_temp:/docker_temp" n'est utile que pour utiliser un dossier temporaire extérieur au docker (donc à priori ce n'est pas nécessaire sur le DIAS).


Note Julien : run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    -v $(pwd)/test_bundle:/test_bundle \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --env csi_si_software_parameter_file=/test_bundle/run_snow_ice_docker.yaml \
    --network=host \
    --name csi_si_software_c \
    csi_si_software:1.0
```

