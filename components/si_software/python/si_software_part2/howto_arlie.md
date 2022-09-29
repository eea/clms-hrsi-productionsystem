# HOWTO ARLIE

## run SQL DB

+ get and unzip the `postgresql.zip` file on the drive (last known adress = `https://drive.google.com/file/d/1zzKK-_KwKmR8vEBW75M1PFA5kD5KByfs/view?usp=sharing`)
+ Then run the database
```
docker run \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=cosims \
    -e POSTGRES_DB=cosims \
    -p 5432:5432 \
    --restart=always \
    -v $(pwd)/postgresql:/var/lib/postgresql/data \
    --name cosims-postgis \
    -d postgis/postgis
```

## run ARLIE S1+S2
```
docker run --rm -it \
    -v path_to/cosims:/work \
    si_software_part2:latest ProcessRiverIce \
    ARLIES1S2 \
    2021-01-24 \
    2021-01-25 \
    /work/part2/ice_s1/compare/RLIES2 \
    /work/part2/ice_s1/compare/RLIES1 \
    /work/part2/ice_s1/compare/RLIES1S2 \
    Rhone \
    /work/data/hidden_value/RiverBasinTiles/Rhone.txt \
    /work/part2/ice_s1/test_arlie/temp \
    /work/part2/ice_s1/test_arlie/appsettings_arlies1s2.json
```
## extract ARLIE

TBD
