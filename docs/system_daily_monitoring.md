# System daily monitoring documentation

The main entry point for high level monitoring operation activites is the 
**Operation Manual** document which is part of the official system documentation.

This documentation is a complement to the **Operation manual**  and is focused 
on internal daily monitoring activities about the the Hr-S&I system, for level 2 
technical operators. It should be used by operators as guidelines to help them 
monitoring the HR-S&I system daily production. 



## System health status

In the HR-S&I Dashboard's **Health** tab : 
- ensure that all the  **internal / external dependencies are up and running**


## Last 24h production summary

In the HR-S&I Dashboard's **Jobs summary** tabs, for every job types (FSC/RLIE, 
RLIE-S1, RLIE-S1-S2, SWS/WDS, GFSC) : 
- ensure that the **number of jobs created** in the last 24h is in a **nominal range** 
(cf. ranges below).
- if the number looks suspicious, futher investigation should be conducted in the 
dahsboard **Jobs list** tabs. Details will be provided in the next section.
- ensure that 100% of the jobs created are either **finished** or **cancelled**, 
if not, make sure that they are being currently processed, and that they are not 
stuck in an **intermediate status**.
- ensure that there are no jobs **stuck in error**. If there are some, refer to 
the **Jobs list** tab to investigate on them, and **restart** the jobs once it's 
done.
- check the **ESA and DIAS turnarounds** to ensure that the jobs are being 
**processed within 3h**.

### Job types nominal production 

The ranges detailed below should not be considered as absolute, the number of jobs 
created from one day to an other may vary. The idea here is to provide mean ranges 
values for a daily production to help operators identify anomalies in the operations.

| Job types | lower job created limit | upper job created limit |
|---|---|---|
| FSC/RLIE | 400 | 600 |
| RLIE-S1 | 80 | 120 |
| RLIE-S1-S2 | 80 | 150 |
| SWS/WDS | 150 | 300 |
| GFSC | 850 | 1150 |

Note that at the time this document was writen, only **one Sentinel-1 satellite** 
was providing data. Therefore, the nominal production ranges may vary if new 
satellites are launched.  


## Database content

In the HR-S&I Dashboard's **Jobs list** tabs, for every job types : 
- ensure there are no jobs **stuck in an intermediate state** (filter out the jobs 
in the following status : `done, internal error, external error, cancelled`). Note 
that some jobs (such as FSC trigerred GFSCs) might spend some time (up to few hours) 
in the `configured` status waiting for input products to be published.
- ensure there are no jobs **stuck in an error state**. If some errors are spotted, 
investigate on the root cause in the Nomad allocation logs (in the **Worker logs** 
tab available after clicking on a job). Restart the jobs in error if its relevant 
(with [patch_database_jobs.py](../common/patch_database_jobs.py) script if many 
jobs are stuck, or with the Dahsboard `Check error` and `Relaunch` buttons otherwise).
- check that the **daily production** didn't face too many **errors** (use **jobs 
with some error** filter).

## Orchestrator services state

Open the **Nomad dashboard** available in the eponym table of the HRSI dashboard, 
and there : 
- ensure all the **orchestrators services are up and running** (`job-creation, 
job-configuration, job-execution, job-publication, worker-pool-management, 
monitor`). To do so, check their logs, and ensure that new ones are displayed 
every few seconds (click on the service name, on its allocation ID, on the task 
name, and finally on the logs tab).
- check if **no error occurred recently** in the services `stderr` logs.
- if a **service is stuck in error**, investigate in the logs to find the root 
cause, and **restart** it if it's relevant. Please use the `Restart` button 
available on the same page than the logs tab, for the **same allocation** to be 
reused and ensure a **continuity in the logs flow**.

Note that you should not refer to the **Nomad status** to identify if an orchestrator 
service is up and running, as they could be stuck in an infinite sleep loop and 
still labelled as **Running** by Nomad.


## Finder API catalog content

Open the Finder portal available at : https://cryo.land.copernicus.eu/finder/, 
and there, for each product type : 
- check that on the previous day the **same number of products** that were 
**published** by the HRSI system are **available in the Finder catalog**.
- check that once you are **logged in** you are **able to download a product** 
(you might want to change the product type you try to download every day).


## WMS Browser catalog content

Open the WMS Browser available at : https://cryo.land.copernicus.eu/browser/#lat=53.54&lng=4.79&zoom=5, 
and there, for each product type : 
- check that on the previous day the **same number of products** that were 
**published** by the HRSI system are **available in the Browser catalog**.
- check that you are **able to display products** on the map within an acceptable 
delay (you might want to change the product type you try to display every day).


## Wekeo portal

Open the WEkEO portal available at : https://www.wekeo.eu/data?view=viewer&t=1642464000000&z=0&center=0%2C24.4152&zoom=10.91, 
and there : 
- add one of the **product type layer** to the map. 
- check that once you are **logged in** you are **able to download a product** 
(you might want to change the product type you try to download every day).


## Operation technical note

If any **anomaly is detected**, you might want to keep track of it in the **document** 
instanciated for this purpose on **google drive**, available there : 
https://docs.google.com/document/d/100rzEw3iFBtJVAL3oeaVwqtoaDgeHwQrYZrTfam7LMk/edit

This document can be used afterwards to provide details, justify production 
perturbation to the client, or in the MQB for instance.


## Known anomalies

Below are detailed common anomalies that have been observed as part of the HRSI 
system monitoring routine.

### System health status

It can happen that the **DIAS API** item appears as **broken**.  
It doesn't necessarily means that the connection with the DIAS is lost for the 
overall HRSI system. It might only comes from the Dashboard which pinged too many 
time the DIAS API in a given time range (note that requests performed from the 
Magellium network might impact that point).  
To ensure that the HRSI operation system is still able to request the DIAS API, 
refer to the **job-creation** service logs in the Nomad dashboard, and make sure 
that requests are performed sucessfully, and that new jobs are being created.

### Last 24h production summary

Some jobs turnarounds might be greater than the 3h requirement because of **DIAS 
network instabilities**. To identify the jobs processing delay root cause, take a 
close look to jobs with high turnaround values in the **Jobs list** tab, and analyze 
where the delay comes from. External errors such as `Request unknown error raised 
during the software run ! error : ('Connection aborted.', ConnectionResetError(104, 
'Connection reset by peer'))` could point in the DIAS network instability direction.

If no, or few jobs jobs have been created in the last 24h, a **warning message** 
should be displayed on top of the **Jobs summary** page starting like this : 
`Warning : an unexpectedly low number of products...`. This could indicates that 
the **creation service** faced an issue, and you might want to check its log to 
ensure that it's running properly. It could also come from an issue on the ESA 
SciHub, or the DIAS side, you can check their APIs as well.

### Database content

If **large batch of FSC** were to be processed at the same time with **temporal 
series of several days in a row**, it can occur that some jobs take as reference 
(`job_id_for_last_valid_l2a`) a job that **won't generate a L2A product**, and 
thus remain stuck as they can't fetch it from the bucket. In such situation, a 
**manual update** of the stuck job **JSON** should be performed to take as 
reference the last job which generated an **L2A product** (and thus an FSC product).

### Finder API catalog content

It can occur that the **total result(s) count** experience trouble to estimate 
the **real number** of products matching the request. If needed check the actual 
number of matching products by iterating over the pages, until you find an **empty 
one**. 

### WMS catalog content

It can occur that some products that have been properly indexed in the Finder 
catalogue **experience trouble to be ingested in the WMS**. In such case, you 
would notice that the number of results matching a similar request would be 
different between the Finder and the WMS Browser. You would then need to notify 
the **WEkEO elasticity support** of this anomaly for them to **re-ingest the missing 
products**. 

### Products download 

The download capability might be unavailble. In such situation you would need to 
notify the **WEkEO elasticity support** of this anomaly. 