# Snow and Ice software (or si_software)

This software contains processing chains for :
+ FSC and RLIE product generation on an S2 tile : __fsc_rlie_processing_chain.py__
+ PSA product generation on an S2 tile : __psa_s2tile_processing_chain.py__
+ PSA LAEA products generation on all S2 tiles in EEA39 (1054 tiles), projected in LAEA coordinate system  : __psa_laea_processing_chain.py__

## Organisation

+ __example_files__ directory contains example of input parameter files for all processing chains
+ __templates__ directory contains :
  + general infos about the program version, helpdesk email, DIAS adress etc... in a general_info.yaml file
  + XML templates to be used for FSC, RLIE, PSA, PSA LAEA and ARLIE products

