# Dates

+ `measurement_date` : average date of measurements on tile
  + `</SENSING_TIME>` from `MTD_TL.xml` in `L1C_MAIN_DIR/GRANULE/subfolder` (UTC).  
    ex:S2B_MSIL1C_20181226T103439_N0207_R108_T32TLR_20181226T122504.SAFE/GRANULE/L1C_T32TLR_A009429_20181226T103436/MTD_TL.xml.
  + first date in __L2A__ name (UTC). a bit less precise (does not contain microseconds).
+ `datatakesensingstart` : sensor acquisition start date
  + `datatakesensingstart` from the Copernicus Scihub request API (UTC)
  + `startDate` or `completionDate` from the Creodias API (UTC)
  + first date in __L1C__ name (UTC). a bit less precise (does not contain microseconds).
  + __WARNING: this is currently named measurement_date in the job handler.__
+ `esa_creation_date` : Copernicus Scihub date of product creation
  + second date in __L1C__ name (UTC). a bit less precise (does not contain microseconds).
+ `esa_publication_date` : Copernicus Scihub publication date
  + `ingestiondate` from the Copernicus Scihub request API (UTC)
+ `dias_publication_date` : DIAS publication date
  + `published` from the Creodias API (UTC)
+ `fsc_cosims_publication_date` : date of product submission by COSIMS to the DIAS
  + `fsc_json_publication_date` on COSIMS DB
+ `fsc_dias_publication_date` : DIAS publication date (from DIAS API)
  + ?? on DIAS API
+ `rlie_cosims_publication_date` : date of product submission by COSIMS to the DIAS
  + `rlie_json_publication_date` on COSIMS DB
+ `rlie_dias_publication_date` : DIAS publication date (from DIAS API)
  + ?? on DIAS API  

Utilities to retrieve dates :

+ Wekeo : object `WekeoS2ProductParser` from parse_s2_products_wekeo.py (si_software)
+ Copernicus : object `CopernicusS2ProductParser` from parse_s2_products_compernicus_scihub.py (si_software)
+ parse L1C for measurement date : function `get_l1c_measurement_date` from chain_processing_request.py (reprocessing). The L1C must of course be available for the tool to look for the XML file and parse it.

__WARNING__ : for dates retrieved from L1C XML file, L2A XML file, copernicus scihub metadata request XML file or dias metadata request JSON file, the length often varies (not the same number of microseconds).
