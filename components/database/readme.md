# Deploy on OpenStack

The OpenStack stack for the database is defined in the `deploy`
folder. Deploy it with:

``` shell
$ cd deploy
$ openstack stack create -t database_stack.yml database_stack
```

# Running a local instance of the database for developpment

First be sure to have docker compose installed on your system.

Build the docker image and instaniate the containers for the database and its
HTTP API:

``` shell
$ sudo ./start_dev_instance.sh
```

This will start the HTTP server and displays requests log on stdout. To stop you
will have to type `Control-C` and use the command `docker-compose rm` to clean
containers (before rebuilding the images).

It is also mandatory to export the DB url variable to the environment, to make it work with the python environement:
``` shell
$ export COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
```

Now you can access and modify jobs using HTTP requests on port 3000 of
localhost. See the [PostgREST
documentetion](http://postgrest.org/en/v6.0/api.html#) for information on the
URL syntax. Here are some examples:

# Using the database with the Swagger UI

Open the URL [http://localhost:8080](http://localhost:8080) from your web browser.

# Using the database with the HTTP interface

The following examples assume the database HTTP API is listening on
`localhost:3000`.

## Get all jobs

``` http
GET http://localhost:3000/parent_jobs HTTP/1.1
```

## Create a job

``` http
POST http://localhost:3000/parent_jobs HTTP/1.1

{
    "tile_id": "32TLS",
    "next_log_level": 10
}
```

Note: It is require to use the header 'Content-Type: application/json'

## Modify a job

Change the status for job wich `id` is 1:

``` http
PATCH http://localhost:3000/parent_jobs?id=eq.1 HTTP/1.1

{ "status": "in_progress" }
```

## Get and filter jobs

Get all pending jobs:
TODO use POST rpc/fsc_rlie_jobs_with_last_status with last_status='{2}' (see job_status.py)

``` http
GET http://localhost:3000/parent_jobs?status=eq.pending HTTP/1.1
```

## Get one job

By default HTTP get on a table returns a list, event if the query string leads
to a unique item:

``` http
GET http://localhost:3000/parent_jobs?id=eq.1 HTTP/1.1
```

returns a list containing one item:

``` json
[
  {
    "id": 1,
    "product_id": "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443",
    "status": "pending"
  }
]
```

To get the item instead of a list, specify `vnd.pgrst.object` as part of the
`Accept` header:

``` http
GET http://localhost:3000/parent_jobs?id=eq.1 HTTP/1.1
Accept: application/vnd.pgrst.object+json
```

which returns:

``` json
{
  "id": 1,
  "product_id": "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443",
  "status": "pending"
}
```