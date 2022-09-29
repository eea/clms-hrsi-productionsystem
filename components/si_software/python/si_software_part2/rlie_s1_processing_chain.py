#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml, dump_yaml
from si_common.follow_process import *
from si_geometry.geometry_functions import *
import si_software.si_logger as si_logger
from si_software_part2.s1_utils import *
from ice_s1_product_final_editing import ice_s1_product_final_editing
from si_software_part2.generate_simulated_rlie_s1_products import generate_simulated_rlie_s1_products

    
class Main_RLIES1_Processing_Information:
    """reads and stores input parameters, handles processing tracking variables"""
    
    def __init__(self, s1grd_path, output_dir, euhydro_shapefile, s2tiles_eea39_gdal_info, hrl_flags, dem_path, precomputed_product_geometries_file=None, temp_dir=None, overwrite=False, \
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
            self.__dico = {'s1grd_path': s1grd_path, 'euhydro_shapefile': euhydro_shapefile, 's2tiles_eea39_gdal_info': s2tiles_eea39_gdal_info, 'hrl_flags': hrl_flags, 'dem_path': dem_path, \
                'precomputed_product_geometries_file': precomputed_product_geometries_file, \
                'timeout': timeout, 'nprocs': nprocs, 'delete_temp_dir_on_success': delete_temp_dir_on_success, 'delete_temp_dir_on_error': delete_temp_dir_on_error}
                
            for key in ['s1grd_path', 'euhydro_shapefile', 's2tiles_eea39_gdal_info', 'hrl_flags']:
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
        
        
        
        
        

def ice_s1_rlie_processing(main_info, simulate_products=False, debug_ice=False):

    main_info.logger_info('')
    main_info.logger_info('')
    main_info.logger_info('##########################################################################################')
    main_info.logger_info('ICE S1')
    main_info.logger_info('')
    main_info.logger_info('ICE S1 preprocessing...')
    
    
    ######################
    #ICE preprocessing
    dico_loc = main_info.input_parameters
    temp_dir_session = main_info.main_temp_dir
    product_information = main_info.product_information
    
    #compute intersecting S2 tiles and get expected S2 tiles and product list
    try:
        dico_s1 = get_s1_product_info(dico_loc['s1grd_path'])
    except Exception as exe:
        print(str(exe))
        raise CodedException('invalid S1 product name %s'%dico_loc['s1grd_path'], exitcode=fsc_rlie_exitcodes.ice_unknown_error)
    if dico_loc['precomputed_product_geometries_file'] is not None:
        #precomputed_product_geometries_file can be used to go through this time consuming step faster since it is needed by both this processing chain and 
        #the pre-processing chain that sets up the necessary static input from the buckets
        product_geometries = load_intersecting_tile_ids_and_geometries(dico_loc['precomputed_product_geometries_file'])
    else:
        dico_loc['precomputed_product_geometries_file'] = os.path.join(temp_dir_session, 'precomputed_product_geometries.json')
        product_geometries = compute_intersecting_tile_ids_and_geometries(dico_loc['s1grd_path'], dico_loc['s2tiles_eea39_gdal_info'], \
            output_file=dico_loc['precomputed_product_geometries_file'], temp_dir=temp_dir_session)
    
    #exit if there are no intersection with S2 tiles containing river and lakes
    if len(product_geometries) < 1:
        main_info.update_processing_status(new_value='ice_success')
        main_info.logger_info('ICE S1 processing successful (no intersection with S2 tiles containing river and lakes)')
        return
            
    main_info.logger_info('')
    main_info.logger_info('Expecting outputs for %d S2 tiles: [%s]'%(len(product_geometries), ','.join(["'%s'"%el for el in sorted(list(product_geometries.keys()))])))
    product_dict_expected = {tile_id: 'RLIE_%s_%s_T%s_%s_%s'%(dico_s1['acquisition_start_date'].strftime('%Y%m%dT%H%M%S'), dico_s1['satellite'], tile_id, \
        product_information['general_info']['product_version'], '1') for tile_id in product_geometries.keys()}
    
    
    #DEM
    dem_files_dict, missing_dem_tiles, has_part1_structure_dem_files = get_dem_20m_paths(dico_loc['dem_path'], list(product_geometries.keys()))
    if len(missing_dem_tiles) > 0:
        raise CodedException('Missing input DEM files:\n%s\n'%('\n'.join([' - %s'%el for el in sorted(list(missing_dem_tiles))])), exitcode=exitcodes.wrong_input_parameters)
        
    #if it has part1 file structure, files must be put into part 2 file structure in a temp directory with symbolic links for ICE processing
    if has_part1_structure_dem_files:
        dem_ice_path = tempfile.mkdtemp(dir=temp_dir, prefix='dem')
        for tile_id in dem_files_dict:
            os.symlink(dem_files_dict[tile_id], os.path.join(dem_ice_path, 'dem_20m_%s.tif'%tile_id))
    else:
        dem_ice_path = dico_loc['dem_path']
            



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
    assert os.path.exists(tile_codes_path), '%s tilecodes file missing'%tile_codes_path
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
        
    ######################
    #ICE processing
    main_info.logger_info('')
    main_info.logger_info('ICE S1 processing...')
    main_info.update_processing_status(new_value='ice_mainprocessing_start')
    ice_product_dir = os.path.join(temp_dir_session, 'out')
    if not simulate_products:
        cmd = [program_path, 'RLIES1', dico_loc['s1grd_path'], dico_loc['precomputed_product_geometries_file'], dico_loc['s2tiles_eea39_gdal_info'], \
            dico_loc['euhydro_shapefile'], dico_loc['hrl_flags'], dem_ice_path, ice_product_dir, os.path.join(temp_dir_session, 'appsettings.json')]
        script_launch_ice = os.path.join(temp_dir_session, 'run_ice.sh')
        txt_launch_ice = ['set -e', 'source /opt/conda/etc/profile.d/conda.sh', 'conda activate ice_env', ' '.join(cmd)]
        with open(script_launch_ice, mode='w') as ds:
            ds.write('%s\n'%('\n'.join(txt_launch_ice)))
        os.chmod(script_launch_ice, stat.S_IRWXU)
        main_info.logger_info('\n'.join(txt_launch_ice))
        execution_dict = execute_commands({'ice': {'cmd': ['bash', script_launch_ice], \
            'stdout_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stdout')], \
            'stderr_write_objects': [main_info.get_subtask_logger_writer(prefix='ICE stderr')]}}, \
            maxtime_seconds=dico_loc['timeout'], scan_dt=1, verbose=main_info.verbose)['ice']
        main_info.logger_info('Ice finished')
    else:
        #simulate products
        start_time_sim = time.time()
        generate_simulated_rlie_s1_products(ice_product_dir, dico_json, dem_ice_path, dico_loc['euhydro_shapefile'], dico_loc['hrl_flags'], \
            dico_loc['s1grd_path'], s2tiles_eea39_gdal_info=dico_loc['s2tiles_eea39_gdal_info'], product_geometries=product_geometries, temp_dir=temp_dir_session, overwrite=False, verbose=main_info.verbose)
        execution_dict = {'exceeded_time': False, 'execution_time': time.time()-start_time_sim, 'returncode': 0, \
            'stdout': ['generate_simulated_rlie_s1_products ran fine'], 'stderr': [], 'forced_exit': False}
    
    ######################
    #ICE postprocessing
    main_info.logger_info('')
    main_info.logger_info('ICE S1 postprocessing...')
    #check ICE success/failure
    
    dump_execution_dict_to_directory(execution_dict, os.path.join(main_info.main_output_dir, 'logs', 'ice'))
    log_ice = '\n'.join(execution_dict['stdout'] + execution_dict['stderr'])
    nointersection_with_land = ('DBG] GRAPH: []' in log_ice) and ('DBG] GDAL: ERROR 1: No input dataset specified.' in log_ice)
    if nointersection_with_land:
        assert execution_dict['returncode'] != 0
        main_info.update_processing_status(new_value='ice_success')
        main_info.logger_info('ICE S1 processing successful (no intersection with land), completed in %s seconds'%execution_dict['execution_time'])
    elif execution_dict['returncode'] == 0:
        
        main_info.update_processing_status(new_value='ice_mainprocessing_success')
        
        #saving ICE product file
        product_ids = set([el.replace('_RLIE.tif', '') for el in os.listdir(ice_product_dir) if '_RLIE.tif' in el])
        product_dict = {el.split('_')[3][1:]: el for el in product_ids}
        missing_tiles = set(product_dict_expected.keys()) - set(product_dict.keys())
        if len(missing_tiles) > 0:
            main_info.logger_info('Some tiles are missing from output:\n%s'%('\n'.join(sorted(list(missing_tiles)))))
        additional_tiles = set(product_dict.keys()) - set(product_dict_expected.keys())
        if len(additional_tiles) > 0:
            main_info.logger_info('Some tiles in output were not expected:\n%s'%('\n'.join(sorted(list(additional_tiles)))))
        missing_products = set(product_dict_expected.values()) - product_ids
        if len(missing_products) > 0:
            main_info.logger_info('Some products are missing from output:\n%s'%('\n'.join(sorted(list(missing_products)))))
        additional_products = product_ids - set(product_dict_expected.values())
        if len(additional_products) > 0:
            main_info.logger_info('Some products in output were not expected:\n%s'%('\n'.join(sorted(list(additional_products)))))
        
        #post-process generated products that were expected
        if debug_ice:
            shutil.copytree(ice_product_dir, os.path.join(main_info.main_output_dir, 'qad'))
        for product_id in product_ids.intersection(set(product_dict_expected.values())):
            tile_id_loc = product_id.split('_')[3][1:]
            os.makedirs(os.path.join(ice_product_dir, product_id))
            for filepath in find_shell(ice_product_dir, expr=product_id+'*', is_file=True):
                shutil.move(filepath, os.path.join(ice_product_dir, product_id, os.path.basename(filepath)))
            ice_s1_product_final_editing(os.path.join(ice_product_dir, product_id), os.path.join(main_info.main_output_dir, 'data', product_id), \
                product_geometries[tile_id_loc], apply_dem_mask_file=dem_files_dict[tile_id_loc])
            main_info.update_product_dict(new_product_keyval_tuple=('rlies1_%s'%tile_id_loc, product_id))
            
        #create status 'ice_success' if all and only expected products were generated
        if len(missing_products) == 0 and len(additional_products) == 0:
            main_info.update_processing_status(new_value='ice_success')
        else:
            main_info.update_processing_status(new_value='ice_productmismatch')
            
        main_info.logger_info('ICE S1 processing successful, completed in %s seconds'%execution_dict['execution_time'])
        
    elif execution_dict['exceeded_time']:
        main_info.update_processing_status(new_value='ice_expired')
        raise CodedException('ICE S1 calculation exceeded %s seconds => terminating csi_si_software'%dico['exec_time_max'], exitcode=exitcodes.subprocess_user_defined_timeout)
    else:
        main_info.update_processing_status(new_value='ice_failed')
        raise CodedException('ICE S1 returned with error %s after %s seconds => terminating csi_si_software\n%s\n'%(execution_dict['returncode'], \
            execution_dict['execution_time'], '\n'.join(execution_dict['stderr'])), exitcode=fsc_rlie_exitcodes.ice_unknown_error)
    




def rlie_s1_processing_chain(s1grd_path, output_dir, euhydro_shapefile, s2tiles_eea39_gdal_info, hrl_flags, dem_path, precomputed_product_geometries_file=None, \
    temp_dir=None, overwrite=False, timeout=None, nprocs=None, verbose=None, simulate_products=False, debug_ice=False, return_with_error_code=False):

    try:
        with Main_RLIES1_Processing_Information(s1grd_path, output_dir, euhydro_shapefile, s2tiles_eea39_gdal_info, hrl_flags, dem_path, \
            precomputed_product_geometries_file=precomputed_product_geometries_file, temp_dir=temp_dir, overwrite=overwrite, timeout=timeout, \
            nprocs=nprocs, verbose=verbose) as main_info:
                
            ##########################################################################################
            #ICE
            ice_s1_rlie_processing(main_info, simulate_products=simulate_products, debug_ice=debug_ice)

    
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
        parser = argparse.ArgumentParser(description='This script is used to launch RLIE generation from S1 product.')
        parser.add_argument("--s1grd", type=str, required=True, help='path to S1 GRD .SAFE folder')
        parser.add_argument("--output_dir", type=str, required=True, help='path to output directory (will be created if missing, must be empty unless --overwrite option is used)')
        parser.add_argument("--euhydro_shapefile", type=str, required=True, help='path to ASTRI eu_hydro_3035.shp shapefile')
        parser.add_argument("--s2tiles_eea39_gdal_info", type=str, required=True, help='path to s2tiles_eea39_gdal_info.json')
        parser.add_argument("--hrl_flags", type=str, required=True, help='path to ASTRI HRL_FLAGS (same as for part 1)')
        parser.add_argument("--dem_path", type=str, required=True, help='path to DEM directory (either containing MAJA structure like for part 1 tile_id/.../.../dem_20m.tif, or directly dem_20m_${tile_id}.tif files)')
        parser.add_argument("--precomputed_product_geometries_file", type=str, help='path to precomputed_product_geometries_file containing {tile_id: geom.wkt} dict')
        parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
        parser.add_argument("--overwrite", action='store_true', help='overwrite output directory if it exists')
        parser.add_argument("--timeout", type=float, help='max processing time allowed in seconds, no limit by default')
        parser.add_argument("--nprocs", type=int, help='number of procs to use, default is 1')
        parser.add_argument("--verbose", type=int, help='verbose level (minimum is 1, default is minimum)')
        parser.add_argument("--simulate_products", action='store_true', help='fakes ProcessRiverIce processing : generates simulated products')
        parser.add_argument("--debug_ice", action='store_true', help='copies ice product to qad folder before si_software post-processing so that they can be analysed later')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    #main function
    rlie_s1_processing_chain(args.s1grd, args.output_dir, args.euhydro_shapefile, args.s2tiles_eea39_gdal_info, args.hrl_flags, args.dem_path, \
        precomputed_product_geometries_file=args.precomputed_product_geometries_file, temp_dir=args.temp_dir, overwrite=args.overwrite, timeout=args.timeout, \
        nprocs=args.nprocs, verbose=args.verbose, simulate_products=args.simulate_products, debug_ice=args.debug_ice, return_with_error_code=False)

