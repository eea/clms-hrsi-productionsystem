#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml, dump_yaml
from si_common.follow_process import *
from si_geometry.geometry_functions import *
from si_geometry.get_valid_data_convex_hull import get_valid_data_convex_hull
import si_software.si_logger as si_logger
from si_software.add_colortable_to_si_products import add_colortable_to_si_products
from si_software.add_quicklook import add_quicklook
from si_software_part2.s1_utils import *

from si_utils.rewrite_cog import rewrite_cog

    
class Main_RLIES12_Processing_Information:
    """reads and stores input parameters, handles processing tracking variables"""
    
    def __init__(self, rlie_s1_dir, rlie_s2_dir, date_to_process, tile_id, output_dir, s2tiles_eea39_gdal_info, temp_dir=None, overwrite=False, \
            timeout=None, nprocs=None, delete_temp_dir_on_success=True, delete_temp_dir_on_error=False, verbose=None):
        """reads and stores input parameters, initializes processing tracking variables"""
        
        self.__dias_mode = False

        #main logger
        #first check some input parameters to get minimal logging started then check all input parameters
        try:
            if verbose is not None:
                self.__verbose_level = verbose
            else:
                self.__verbose_level = 1
            assert isinstance(self.__verbose_level, int)
            if self.__verbose_level < 1:
                self.__verbose_level = 1
        except:
            raise MainArgError('verbose parameter not filled properly')
        self.__logger = si_logger.get_logger(log_file_path=None, verbose_level=self.__verbose_level)
        
            
        self.__exec_date = datetime.utcnow()
        
        ####################
        #initialize folder structure and logger, read and store input parameters
        #main output dir
        self.__main_output_dir = os.path.abspath(output_dir)
        #check that output folder is empty if it exists, and create it if it does not exist
        if os.path.exists(self.__main_output_dir):
            if overwrite:
                shutil.rmtree(self.__main_output_dir)
            elif len(os.listdir(self.__main_output_dir)) > 0:
                raise Exception('output directory must be empty or --overwrite option must be used')
        os.makedirs(self.__main_output_dir, exist_ok=True)

        #create main_output_dir subdirectory structure
        self.__log_dir = os.path.join(self.__main_output_dir, 'logs')
        self.__data_dir = os.path.join(self.__main_output_dir, 'data')
        make_folder(self.__log_dir)
        make_folder(self.__data_dir)
        
        #add filehandler to logger (saves log to file)
        self.__log_file = os.path.join(self.__log_dir, 'csi_si_software.log')
        self.__logger = si_logger.LogUtil.add_file_handler(self.__logger, self.__log_file)    
        
        #temp dir
        if temp_dir is not None:
            temp_dir = os.path.abspath(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
        self.__main_temp_dir = tempfile.mkdtemp(prefix='rlies1_', dir=temp_dir)
        
        #check parameters remaining
        try:
            self.__dico = {'rlie_s1_dir': rlie_s1_dir, 'rlie_s2_dir': rlie_s2_dir, 'date_to_process': date_to_process, 'tile_id': tile_id, \
                's2tiles_eea39_gdal_info': s2tiles_eea39_gdal_info, 'timeout': timeout, 'nprocs': nprocs, \
                'delete_temp_dir_on_success': delete_temp_dir_on_success, 'delete_temp_dir_on_error': delete_temp_dir_on_error}
                
            for key in ['rlie_s1_dir', 'rlie_s2_dir']:
                assert os.path.exists(self.__dico[key])
                
            if self.__dico['timeout'] is not None:
                assert self.__dico['timeout'] > 0.
                            
            if self.__dico['nprocs'] is None:
                self.__dico['nprocs'] = 1
            else:
                assert isinstance(self.__dico['nprocs'], int)
            if self.__dico['nprocs'] < 1:
                self.__dico['nprocs'] = 1
                
            for key in ['delete_temp_dir_on_success', 'delete_temp_dir_on_error']:
                assert isinstance(self.__dico[key], bool)

        except CodedException as ex:
            print('CodedException error in check parameters')
            raise ex
        except AssertionError as ex:
            print('AssertionError error in check parameters')
            raise CodedException(str(ex), exitcode=exitcodes.wrong_input_parameters)
        except:
            print('unknown error in check parameters')
            raise
            
        try:            
            
            ####################
            #initialize processing tracking variables
            
            #get processing status info
            self.__processing_status_file = os.path.join(self.main_output_dir, 'status.yaml')
            self.update_processing_status(initialize=True)
                
            #product dict and file
            self.__product_file = os.path.join(self.__data_dir, 'product_dict.yaml')
            self.update_product_dict(initialize=True)
            
            #product information needed to fill product inspire compliant XML files for registration on DIAS
            self.__initialize_product_information_from_template()
            ####################
        except CodedException as ex:
            print('CodedException error in init')
            raise ex
        except AssertionError as ex:
            print('assertion error in init')
            raise InnerArgError(str(ex))
        except:
            print('unknown error in init')
            raise

        
    def __enter__(self):
        """enter context manager: this allows for the logger to catch errors automatically since the __exit__() method is triggered"""
        return self
        

    def __exit__(self, exit_type, exit_value, exit_traceback):
        """exits context"""
        if exit_value is None:
            self.update_processing_status(new_value='exiting_completed')
            if self.__dico['delete_temp_dir_on_success']: #delete temporary folder
                self.__delete_main_temporary_folder()
        else:
            self.__logger.error('', exc_info=True)
            self.update_processing_status(new_value='exiting_error')
            if self.__dico['delete_temp_dir_on_error']: #delete temporary folder
                self.__delete_main_temporary_folder()

        
    def update_processing_status(self, initialize=False, new_value=None):
        """Update processing status: update processing statuses list and status.yaml file and keeps them coherent"""
        try:
            assert (new_value is None) or (not initialize), 'in update_processing_status, new_value and initialize parameters cannot both be filled'
            if new_value is not None:
                self.__processing_statuses.append(new_value)
                self.__write_processing_statuses_to_file()
                self.dias_logger_info(new_value)
                self.logger_info('status change: %s'%new_value)
            elif initialize:
                if os.path.exists(self.__processing_status_file):
                    self.__processing_statuses = load_yaml(self.__processing_status_file, env_vars=False)['status']
                    if self.__processing_statuses is None:
                        self.__processing_statuses = []
                    self.__processing_statuses.append('rlies1processingchain_restart')
                else:
                    self.__processing_statuses = ['rlies1processingchain_start']
                    self.__write_processing_statuses_to_file()
            else:
                raise InputException('in update_processing_status either new_value or initialize parameter must be filled')
        except CodedException as ex:
            raise ex
        except AssertionError as ex:
            raise InnerArgError(str(ex))
            
            
    def __write_processing_statuses_to_file(self):
        """write processing statuses to file"""
        with open(self.__processing_status_file, mode='w') as ds:
            ds.write('status:\n%s\n'%('\n'.join(['- %s'%el for el in self.__processing_statuses])))
        
        
    def update_product_dict(self, initialize=False, new_product_keyval_tuple=None):
        """Update product dict: update product dict and product_dict.yaml file and keeps them coherent"""
        try:
            assert (new_product_keyval_tuple is None) or (not initialize), 'in update_processing_status, new_value and initialize parameters cannot both be filled'
            if new_product_keyval_tuple is not None:
                product_path = os.path.join(self.__data_dir, new_product_keyval_tuple[1])
                assert len(new_product_keyval_tuple) == 2, 'expecting a key, value tuple for new product insertion'
                self.__product_dict[new_product_keyval_tuple[0]] = new_product_keyval_tuple[1]
                self.__write_product_dict_to_file()
            elif initialize:
                if os.path.exists(self.__product_file):
                    self.__product_dict = load_yaml(self.__product_file, env_vars=False)
                    if self.__product_dict is None:
                        self.__product_dict = dict()
                    for key, value in self.__product_dict.items():
                        product_path = os.path.join(self.__data_dir, value)
                        if not os.path.exists(product_path):
                            raise MainInputFileError('product %s is present in product dict but file cannot be found at path %s'%(key, product_path))
                else:
                    self.__product_dict = dict()
                    self.__write_product_dict_to_file()
            else:
                raise InputException('in update_product_dict either new_product_keyval_tuple or initialize parameter must be filled')
        except CodedException as ex:
            raise ex
        except AssertionError as ex:
            raise InnerArgError(str(ex))
            
        
    def __write_product_dict_to_file(self):
        """write product dict to file"""
        with open(self.__product_file, mode='w') as ds:
            ds.write('%s\n'%('\n'.join(['%s: %s'%(key, value) for key, value in self.__product_dict.items()])))
                
    
    def __initialize_product_information_from_template(self):
        """load general information into common_info dict"""
        self.__product_information = dict()
        general_info_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'general_info.yaml')
        assert os.path.exists(general_info_file), 'could not find general info file on si_software install directory at %s'%general_info_file
        self.__product_information['general_info'] = load_yaml(general_info_file, env_vars=False)
        self.__product_information['production_date'] = self.__exec_date
        
    def dias_logger_info(self, msg):
        """DIAS logger: only activated on DIAS and is used to send status information"""
        if self.__dias_mode:
            self.__dias_logger.info(msg)
        
    def logger_info(self, msg):
        self.__logger.info(msg)
        
    def logger_warning(self, msg):
        self.__logger.warning(msg)
        
    def logger_error(self, msg, exc_info=True):
        self.__logger.error(msg, exc_info=True)
        
    def __delete_main_temporary_folder(self):
        #WARNING: this routine can delete a whole folder structure recursively
        shutil.rmtree(self.__main_temp_dir)
        
    def get_subtask_logger_writer(self, prefix=None):
        return SimpleWriteLinesToLoggerDebug(self.__logger, prefix=prefix)
        
    def get_product_path(self, key, check_exists=True):
        assert key in self.__product_dict, 'product %s does not exist'%key
        output_path = os.path.join(self.__data_dir, self.__product_dict[key])
        if check_exists:
            assert os.path.exists(output_path), 'product %s is in product dict but does not exist at path %s'%(key, output_path)
        return output_path
        
    @property
    def main_output_dir(self):
        return self.__main_output_dir
    @property
    def main_temp_dir(self):
        return self.__main_temp_dir
    @property
    def verbose(self):
        return self.__verbose_level
    @property
    def status_file(self):
        return self.__status_file
    @property
    def exec_date(self):
        return self.__exec_date
    @property
    def product_dict(self):
        return copy.deepcopy(self.__product_dict)
    @property
    def templates(self):
        return copy.deepcopy(self.__templates)
    @property
    def product_information(self):
        return copy.deepcopy(self.__product_information)
    @property
    def input_parameters(self):
        return copy.deepcopy(self.__dico)
        



def ice_s1s2_product_final_editing(ice_product_input_dir, ice_product_output_dir, wekeo_geom):
    
    product_id = os.path.basename(ice_product_input_dir)
    files_expected = set()
    for expected_sufix in ['_RLIE.tif', '_QC.tif', '_QCFLAGS.tif', '_MTD.xml']:
        file_loc = os.path.join(ice_product_input_dir, product_id + expected_sufix)
        assert os.path.exists(file_loc), 'file %s missing from product'%file_loc
        files_expected.add(product_id + expected_sufix)
    #remove unexpected files
    for filename in set(os.listdir(ice_product_input_dir)) - files_expected:
        os.unlink(os.path.join(ice_product_input_dir, filename))
    
    #add color tables to tif files
    add_colortable_to_si_products(ice_product_input_dir, product_tag=product_id)
    
    #transform geotiff into COG
    rewrite_cog(ice_product_input_dir, dest_path=ice_product_output_dir, verbose=1)
    
    #add quicklook
    add_quicklook(ice_product_output_dir, '_RLIE.tif')
    
    measurement_date = datetime.strptime(product_id.split('_')[1], '%Y%m%dT%H%M%S')
    
    json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": wekeo_geom.wkt
            },
            "properties": {
                "productIdentifier": None,
                "title": product_id,
                "resourceSize": 1024*int(compute_size_du(ice_product_output_dir)),
                "organisationName": "EEA",
                "startDate": measurement_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "completionDate": measurement_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "productType": "RLIE",
                "resolution": 20,
                "mission": 'S1-S2',
                "processingBaseline": product_id.split('_')[-2],
                "host_base": None,
                "cloudCover": None,
                "s3_bucket": None,
                "thumbnail": None
            }}}
            
    with open('%s/dias_catalog_submit.json'%ice_product_output_dir, mode='w') as ds:
        json.dump(json_dict, ds, ensure_ascii=True, indent=4)
        
        
        
        

def ice_s1s2_rlie_processing(main_info):

    main_info.logger_info('')
    main_info.logger_info('')
    main_info.logger_info('##########################################################################################')
    main_info.logger_info('ICE S1 + S2')
    main_info.logger_info('')
    main_info.logger_info('ICE S1 + S2 preprocessing...')
    
    
    ######################
    #ICE preprocessing
    dico_loc = main_info.input_parameters
    temp_dir_session = main_info.main_temp_dir
    product_information = main_info.product_information
    tile_id = dico_loc['tile_id']
    
    #get S1 and S2 products
    rlie_s2_products = sorted([el for el in find_shell(dico_loc['rlie_s2_dir'], expr='RLIE_%sT*_T%s*'%(dico_loc['date_to_process'].strftime('%Y%m%d'), tile_id), is_dir=True, is_file=False) \
        if len(os.path.basename(el).split('_')) == 6 and os.path.basename(el).split('_')[3][1:] == tile_id])[::-1]
    print('RLIE S2 products:\n%s'%('\n'.join([' - %s'%el for el in rlie_s2_products])))
    rlie_s1_products = sorted([el for el in find_shell(dico_loc['rlie_s1_dir'], expr='RLIE_%sT*_T%s*'%(dico_loc['date_to_process'].strftime('%Y%m%d'), tile_id), is_dir=True, is_file=False) \
        if len(os.path.basename(el).split('_')) == 6 and os.path.basename(el).split('_')[3][1:] == tile_id])[::-1]
    print('RLIE S1 products:\n%s'%('\n'.join([' - %s'%el for el in rlie_s1_products])))
    
    #create S1 and S2 product directories with symbolic links to RLIE S1 and RLIE S2 products chosen
    rlie_s2_dir_temp = os.path.join(temp_dir_session, 'RLIE_S2')
    os.makedirs(rlie_s2_dir_temp)
    for el in rlie_s2_products:
        os.symlink(el, os.path.join(rlie_s2_dir_temp, os.path.basename(el)))
    
    rlie_s1_dir_temp = os.path.join(temp_dir_session, 'RLIE_S1')
    os.makedirs(rlie_s1_dir_temp)
    for el in rlie_s1_products:
        os.symlink(el, os.path.join(rlie_s1_dir_temp, os.path.basename(el)))
    
    
    #get product geometries
    with open(dico_loc['s2tiles_eea39_gdal_info']) as ds:
        dico_tiles = json.load(ds)
    assert tile_id in dico_tiles
    product_geometry_s1s2 = RasterPerimeter(dico_tiles[tile_id]).projected_perimeter('epsg:4326', npoints_per_edge=3)
    

    program_path = shutil.which('ProcessRiverIce')
    program_dir_path = os.path.dirname(program_path)
    
    #graph files
    graph_dir_path = os.path.join(program_dir_path, 'graphs')
    graph_files = {'main': os.path.join(graph_dir_path, 'preprocessing_classification_fre_5class_md20m_1input_14flat_bsi_cut_auto.xml'), \
        's1': os.path.join(graph_dir_path, 'grd_preprocessing_threshold_speckle_filtering_Sim_subs_rep_2_auto.xml')}
    for path_loc in graph_files.values():
        assert os.path.exists(path_loc), '%s graph file missing'%path_loc
        
    #template files
    tile_codes_path = os.path.join(program_dir_path, 'TileCodes', 'TileCodes.txt')
    template_dir_path = os.path.join(program_dir_path, 'metadata')
    template_files = {'rlie_s2': os.path.join(template_dir_path, 'RLIE_Metadata.xml'), \
        'rlie_s1': os.path.join(template_dir_path, 'RLIE_S1_Metadata.xml'), \
        'rlie_s1s2': os.path.join(template_dir_path, 'RLIE_S1S2_Metadata.xml'), \
        'arlie_s2': os.path.join(template_dir_path, 'ARLIE_Metadata.xml')}
    for path_loc in template_files.values():
        assert os.path.exists(path_loc), '%s template file missing'%path_loc
        
    #edit appsettings.json file
    dico_json = {"Configuration" : {"SnapPath": shutil.which('gpt'), \
            "TileCodes": tile_codes_path, \
            "GraphPath": graph_files['main'], \
            "GraphS1Path": graph_files['s1'], \
            "RlieMetadataTemplatePath": template_files['rlie_s2'], \
            "RlieS1MetadataTemplatePath": template_files['rlie_s1'], \
            "RlieS1S2MetadataTemplatePath": template_files['rlie_s1s2'], \
            "ArlieMetadataTemplatePath": template_files['arlie_s2'], \
            "MaxThreads": dico_loc['nprocs'], \
            "ProductVersion": product_information['general_info']['product_version'], \
            "GenerationMode": 1, \
            "HelpDeskEmail": product_information['general_info']['helpdesk_email'], \
            "PumUrl": product_information['general_info']['pum_url'], \
            "DiasUrl": product_information['general_info']['dias_url'], \
            "DiasPortalName": product_information['general_info']['dias_portal_name'], \
            "ValidationReportFilename": 'hrsi-ice-qar', \
            "ValidationReportDate": product_information['general_info']['report_date']}}
    dico_json['Serilog'] = {'MinimumLevel': 'Debug', 'WriteTo': [{'Name': 'Async', 'Args': {'configure': [{'Name': 'Console', \
        "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}] [{SourceContext}] [{EventId}] {Message}{NewLine}{Exception}"}]}}]}
    with open(os.path.join(temp_dir_session, 'appsettings.json'), mode='w') as ds:
        json.dump(dico_json, ds)
        
    # ~ print(json.dumps(dico_json, indent=4))
        
    ######################
    #ICE processing
    main_info.logger_info('')
    main_info.logger_info('ICE S1 processing...')
    main_info.update_processing_status(new_value='ice_mainprocessing_start')
    ice_product_dir = os.path.join(temp_dir_session, 'out')
    os.makedirs(ice_product_dir)
    cmd = [program_path, 'RLIES1S2', dico_loc['date_to_process'].strftime('%Y-%m-%d'), 'T' + tile_id, rlie_s2_dir_temp, rlie_s1_dir_temp, ice_product_dir, os.path.join(temp_dir_session, 'appsettings.json')]
    execution_dict = execute_commands({'ice': {'cmd': cmd, \
        'stdout_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stdout')], \
        'stderr_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stderr')]}}, \
        maxtime_seconds=dico_loc['timeout'], scan_dt=1, verbose=main_info.verbose)['ice']
        
    

    
    ######################
    #ICE postprocessing
    main_info.logger_info('')
    main_info.logger_info('ICE S1 + S2 postprocessing...')
    #check ICE success/failure
    
    dump_execution_dict_to_directory(execution_dict, os.path.join(main_info.main_output_dir, 'logs', 'ice'))
    if execution_dict['returncode'] == 0:
        
        main_info.update_processing_status(new_value='ice_mainprocessing_success')
        
        product_ids = set([el.replace('_RLIE.tif', '') for el in os.listdir(ice_product_dir) if '_RLIE.tif' in el])
        assert len(product_ids) == 1
        product_id = list(product_ids)[0]
        
        #post-process generated products that were expected
        tile_id_loc = product_id.split('_')[3][1:]
        os.makedirs(os.path.join(ice_product_dir, product_id))
        for filepath in find_shell(ice_product_dir, expr=product_id+'*', is_file=True):
            shutil.move(filepath, os.path.join(ice_product_dir, product_id, os.path.basename(filepath)))
        ice_s1s2_product_final_editing(os.path.join(ice_product_dir, product_id), os.path.join(main_info.main_output_dir, 'data', product_id), product_geometry_s1s2)
        main_info.update_product_dict(new_product_keyval_tuple=('rlies1_%s'%tile_id_loc, product_id))
        main_info.update_processing_status(new_value='ice_success')
        main_info.logger_info('ICE S1 processing successful, completed in %s seconds'%execution_dict['execution_time'])
    elif execution_dict['exceeded_time']:
        main_info.update_processing_status(new_value='ice_expired')
        raise CodedException('ICE S1 calculation exceeded %s seconds => terminating csi_si_software'%dico['exec_time_max'], exitcode=exitcodes.subprocess_user_defined_timeout)
    else:
        main_info.update_processing_status(new_value='ice_failed')
        raise CodedException('ICE S1 returned with error %s after %s seconds => terminating csi_si_software\n%s\n'%(execution_dict['returncode'], \
            execution_dict['execution_time'], '\n'.join(execution_dict['stderr'])), exitcode=fsc_rlie_exitcodes.ice_unknown_error)
    





def rlie_s1s2_processing_chain(rlie_s1_dir, rlie_s2_dir, date_to_process, tile_id, output_dir, s2tiles_eea39_gdal_info, temp_dir=None, \
    overwrite=False, timeout=None, nprocs=None, verbose=None, return_with_error_code=False):

    try:
        with Main_RLIES12_Processing_Information(rlie_s1_dir, rlie_s2_dir, date_to_process, tile_id, output_dir, s2tiles_eea39_gdal_info, temp_dir=temp_dir, \
            overwrite=overwrite, timeout=timeout, nprocs=nprocs, verbose=verbose) as main_info:
                
            ##########################################################################################
            #ICE
            ice_s1s2_rlie_processing(main_info)

    
    except CodedException as ex:
        if return_with_error_code:
            return ex.exitcode
        ex.exit(print_traceback=True)
    except InterruptedError as ex:
        if return_with_error_code:
            return exitcodes.kill_2
        sys.exit(exitcodes.kill_2)
    except KeyboardInterrupt as ex:
        if return_with_error_code:
            return exitcodes.kill_2
        sys.exit(exitcodes.kill_2)
    except OSError as ex:
        if return_with_error_code:
            return exitcodes.os_error
        sys.exit(exitcodes.os_error)
    except MemoryError as ex:
        if return_with_error_code:
            return exitcodes.memory_limit
        sys.exit(exitcodes.memory_limit)
    except Exception as ex:
        if return_with_error_code:
            return exitcodes.default
        print(str(ex))
        sys.exit(exitcodes.default)
    except:
        if return_with_error_code:
            return exitcodes.default
        print('Exiting with uncatchable error')
        raise
        sys.exit(exitcodes.default)
            
    if return_with_error_code:
        return 0

    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to launch RLIE generation from S1 + S2 products.')
        parser.add_argument("--rlie_s1_dir", type=str, required=True, help='path to directory containing RLIE S1 products')
        parser.add_argument("--rlie_s2_dir", type=str, required=True, help='path to directory containing RLIE S2 products')
        parser.add_argument("--date_to_process", type=str, required=True, help='date_to_process in %Y-%m-%d format')
        parser.add_argument("--tile_id", type=str, required=True, help='tile ID. Ex: 32TLR')
        parser.add_argument("--output_dir", type=str, required=True, help='path to output directory (will be created if missing, must be empty unless --overwrite option is used)')
        parser.add_argument("--s2tiles_eea39_gdal_info", type=str, required=True, help='path to s2tiles_eea39_gdal_info.json')
        parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
        parser.add_argument("--overwrite", action='store_true', help='overwrite output directory if it exists')
        parser.add_argument("--timeout", type=float, help='max processing time allowed in seconds, no limit by default')
        parser.add_argument("--nprocs", type=int, help='number of procs to use, default is 1')
        parser.add_argument("--verbose", type=int, help='verbose level (minimum is 1, default is minimum)')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    args.date_to_process = datetime.strptime(args.date_to_process, '%Y-%m-%d')
        
    #main function
    rlie_s1s2_processing_chain(args.rlie_s1_dir, args.rlie_s2_dir, args.date_to_process, args.tile_id, args.output_dir, args.s2tiles_eea39_gdal_info, \
        temp_dir=args.temp_dir, overwrite=args.overwrite, timeout=args.timeout, nprocs=args.nprocs, verbose=args.verbose, return_with_error_code=False)

