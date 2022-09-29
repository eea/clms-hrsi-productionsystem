docker run --name arlie-postgis -e POSTGRES_PASSWORD=cosims -d postgis/postgis
#~ docker run -it --link arlie-postgis:postgres --rm postgres sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U postgres'
