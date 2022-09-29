# Job status transitions flow chart

``` mermaid
graph TD
A[ initialized ] -->
B[ configured ] -->
C[ ready ] -->
D[ queued ] -->
E[ started ] -->
F[ pre_processing ] -->
G[ processing ] -->
H[ post_processing ] -->
I[ processed ] -->
J[ start_publication ] -->
K[ published ] -->
W[ done ]

L[ internal_error ]
M[ external_error ]
N[ error_checked ]
O[ cancelled ]

P{ Error occured? }
P --> | yes | Q
P --> | no | T
Q{ external<br>error? }
Q --> | yes | M
Q --> | no | L
S{ threshold<br>value<br>reached? }
S --> | yes | L
S --> | no | N
T( Move to next state )
U( Run process again )

M --> S
N --> U
```

# Job status description

## Initialized

A new job has been created by the "job_creation" service. This one being triggered
when a new input product is available (L1C, S1-GRD, FSC, RLIE-S1, ...), for any
type of job (FSC/RLIE, RLIE-S1, RLIE-S1+S2, SWS/WDS, GFSC). This freshly created
job is waiting to be configured.

## Configured

The studied job has been successfully configured by the "job_configuration" service.
We evaluate if it depends on an other older job, which focuses on the same tile.
If it does, the current job will remain in this state until the one it depends on
is `processed`.

## Ready

The job didn't depend on any other job, or these dependencies were satisfied.
It's then waiting to be processed.

## Queued

The current job has been placed in a queue, waiting to be affected to an available
worker, by the "job_execution" service. The "Nomad" tool is in charge of executing
the job in the worker, providing both job's and input product's IDs.

## Started

The job has been successfully affected to an available worker. The worker, on
which the job will be executed, has been properly initialized, with required
attributes and environment variables.

## Pre-Processing

Information relative to the studied job are collected, the input product and
auxiliary data are downloaded in the appropriate location for the processing
software. The environment is also set for the next step.

## Processing

The scientific software is run and some Snow and/or Ice products are generated
locally on the worker.

## Post-Processing

Upload the generated Snow and/or Ice products on the HR-S&I product bucket.
Update some of the job information and clean the worker environment after the
job processing.

## Processed

The job has been successfully processed.

## Start Publication

Starting the publication of the processed results to the DIAS.

## Published

The results processed from the job execution have been successfully  published
to the DIAS.

## Internal Error

An error occured either in the system's logic, or in an internal dependency.
It could require a code maintenance, if needed the system is stopped, otherwise
it keeps running. If this state is set during the job processing, we
automatically try to run it again with a limit of 3 attempt (in case the issue
would be temporary).

## External Error

An error occured in the system's external dependencies (DIAS, ESA hub, etc.).
As we are not able to fix the issue on our own the system keeps running, hoping
that the dependency will be fixed.

## Error Checked

The error was not critic for the system and could be handled. An other attempt
to process the current job will be done, starting from the step which threw
the error, unless the threshold value has been reached.

## Cancelled

The job's input product has been published as part of a reprocessing campaign
for a products which was measured before the operational system start date.
Therefore, we will keep track of this product in the system, but won't process
it. In case of GFSC jobs, if a job is detected as redundant of a jobs already
indexed in the database which data is more up to date than the current one,
the new job will be moved to `cancelled` status.
