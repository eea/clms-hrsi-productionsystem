# MAJA mode transitions flow chart

## Starting with Init mode

``` mermaid
graph TD
A( No recent product<br>available ) -->
B[ init ] -->
C( Process one product ) -->
D[ nominal 'degraded' ] -->
E{ 'N' products<br>processed<br>since init? }
E --> | yes | F
E --> | yes | I
E --> | no | D
F[ nominal ] -->
G{ Old product<br>to process? }
G --> | yes | I
G --> | no | H
H{ Recent product<br>available? }
H --> | yes | F
H --> | no | B

I[ backward ] --> G
```

## Starting with Backward mode

``` mermaid
graph TD
A( Old products ) -->
B[ backward ] -->
C( Backward completed ) -->
D[ nominal ] -->
E{ Old product<br>to process? }
E --> | yes | B
E --> | no | F
F{ Recent product<br>available? }
F --> | yes | D
F --> | no | G
G[ init ] -->
H( Process one product ) -->
I[ nominal 'degraded' ] -->
J{ 'N' products<br>processed<br>since init? }
J --> | yes | D
J --> | yes | B
J --> | no | I
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
