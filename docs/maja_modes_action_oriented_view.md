# Action oriented view for handling MAJA modes

``` mermaid
graph TD
init(Service initialization<br>tmin = tnow - N days. N=60 should be enough.<br>ti = date of first L1C found after tmin with low L1C cloud cover value) -->
bcond0{L1C i+7 exists?}
reprocesschoice{L2A i-7 is degraded product?}
cloudchoice{L2A i not produced i.e. too cloudy?}
bcond1{L1C time deltas ti:ti+7 < 60 days?}
dateignored[ mark date ti as ignored from now on ]
initmode[ init mode ]
backwardmode[ backward mode ]
nominalmode[ nominal mode ]
stepup(i += 1)
bcond0 --> | yes | bcond1
bcond0 --> | no | initmode
bcond1 --> | yes | backwardmode
bcond1 --> | no | initmode
backwardmode --> reprocesschoice
initmode --> reprocesschoice
nominalmode --> reprocesschoice
reprocesschoice -->  | no | cloudchoice
cloudchoice -->  | no | stepup
cloudchoice -->  | yes | dateignored
dateignored -->  stepup
reprocesschoice --> | yes | bcond1
stepup --> nominalmode
```

# MAJA modes description

## Init

Mode used when no recent product is available. With this mode, MAJA is able to
compute a degraded L2A product without receiving as an input the previous
product's L2A. The products following one run with MAJA init mode will also
produce degraded L2A. The quality improve as new products are computed, and
is not considered degraded anymore after a threshold value of processed L2A
is reached.

## Nominal

This mode is set when recent products are available. They are considered recent
if the measurement date difference between the job being cconfigured and the
one already being processed is inferior to a threshold value. In this mode, we
can provide the latest L2A product to the current job, for it to produce a good
quality result.

## Backward

This mode is used to process or reprocess old products. It can be triggered
because a product has been delayed and is handled long after its measurement,
or to improve the result quality of a product which has been previously processed
with MAJA in "Init" mode. It requires that several consecutives products, more
recent than the one being configured, were already processed, to use the produced
L2A results.
