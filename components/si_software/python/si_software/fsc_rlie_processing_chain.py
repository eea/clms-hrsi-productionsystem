#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.follow_process import SimpleWriteLinesToLoggerDebug
from si_common.yaml_parser import load_yaml, dump_yaml

from si_geometry.geometry_functions import set_gdal_otb_itk_env_vars

import si_software.si_logger as si_logger
from si_software.maja_l2a_processing import maja_l2a_processing, update_product_information_from_l2a_file, clean_l2a_keep_metadata
from si_software.lis_fsc_processing import lis_fsc_processing
from si_software.ice_rlie_processing import ice_rlie_processing


    
def check_dem_integrity(dem_path):
    if not os.path.exists(dem_path):
        raise MainInputFileError('dem not found at path %s'%dem_path)
        

def check_input_maja_options(dico):
    subdict = dico['maja']
    check_dict(subdict, ['static_parameter_files_dir', 'user_config_dir', 'mode', 'l1c_file', 'save_output_l2a_file'], check_none=True)
    check_dict(subdict, ['product_mode_overide', 'l2a_file', 'max_processing_time', 'max_cloud_cover_acceptable_percent'], check_none=False)
    if not isinstance(subdict['save_output_l2a_file'], bool):
        raise MainArgError('execting boolean for input parameter save_output_l2a_file')
    if subdict['max_cloud_cover_acceptable_percent'] is None:
        subdict['max_cloud_cover_acceptable_percent'] = 100.
    subdict['max_cloud_cover_acceptable_percent'] = float(subdict['max_cloud_cover_acceptable_percent'])
    assert 0. <= subdict['max_cloud_cover_acceptable_percent'] <= 100., 'max_cloud_cover_acceptable_percent parameter must be in [0,100]'
    if subdict['max_processing_time'] is not None:
        subdict['max_processing_time'] = float(subdict['max_processing_time'])
    if subdict['mode'] == 'nominal':
        if subdict['l2a_file'] is None:
            raise MainArgError('input l2a_file must be specified for MAJA nominal mode')
        for el in ['l1c_file', 'l2a_file']:
            if not os.path.exists(subdict[el]):
                raise MainArgError('%s not found at path %s'%(el, subdict[el]))
        assert subdict['l1c_file'].split('_')[-2][1:] == dico['tile_name'], 'l1c file %s does not match tile_name %s'%(subdict['l1c_file'], dico['tile_name'])
        assert subdict['l2a_file'].split('_')[-3][1:] == dico['tile_name'], 'l2a file %s does not match tile_name %s'%(subdict['l2a_file'], dico['tile_name'])
    elif subdict['mode'] == 'init':
        if not os.path.exists(subdict['l1c_file']):
            raise MainArgError('l1c file not found at path %s'%subdict['l1c_file'])
        if subdict['l2a_file'] is not None:
            raise MainArgError('L2A file cannot be specified when MAJA init mode is used')
        assert subdict['l1c_file'].split('_')[-2][1:] == dico['tile_name'], 'l1c file %s does not match tile_name %s'%(subdict['l1c_file'], dico['tile_name'])
    elif subdict['mode'] == 'backward':
        if subdict['l2a_file'] is not None:
            raise MainArgError('L2A file cannot be specified when MAJA backward mode is used')
        subdict['l1c_file'] = list_form(subdict['l1c_file'])
        for el in subdict['l1c_file']:
            if not os.path.exists(el):
                raise MainArgError('l1c file not found at path %s'%el)
            assert el.split('_')[-2][1:] == dico['tile_name'], 'l1c file %s does not match tile_name %s'%(el, dico['tile_name'])
    else:
        raise MainArgError('mode %s unknown'%subdict['mode'])
    if 'additional_maja_options' not in subdict:
        subdict['additional_maja_options'] = None
    if 'debug' in subdict:
        if not isinstance(subdict['debug'], bool):
            raise MainArgError('execting boolean for input parameter debug')
        if subdict['debug']:
            if subdict['additional_maja_options'] is None:
                subdict['additional_maja_options'] = '--perfos-log --perfos-report -l DEBUG'
            else:
                raise MainArgError('conflicting options for MAJA: debug: true and additional_maja_options filled')
        del subdict['debug']
        
    #product ouput mode
    if subdict['product_mode_overide'] is None:
        subdict['product_mode_overide'] = 0
        if subdict['mode'] == 'nominal':
            subdict['product_mode_overide'] = 1
        elif subdict['mode'] == 'backward':
            if len(subdict['l1c_file']) >= 8:
                subdict['product_mode_overide'] = 1
    
    if 'remove_sre_bands' not in subdict:
        subdict['remove_sre_bands'] = True
    if not isinstance(subdict['remove_sre_bands'], bool):
        raise MainArgError('execting boolean for input parameter remove_sre_bands')
    
    return dico



    
class Main_FSC_RLIE_Processing_Information:
    """reads and stores input parameters from dico_in, handles processing tracking variables"""
    
    def __init__(self, dico_in):
        """reads and stores input parameters from dico_in, initializes processing tracking variables"""

        #main logger
        #first check some input parameters to get minimal logging started then check all input parameters
        try:
            self.__verbose_level = 1
            if 'verbose' in dico_in:
                self.__verbose_level = get_and_del_param(dico_in, 'verbose')
            assert isinstance(self.__verbose_level, int)
        except:
            raise MainArgError('verbose parameter not filled properly')
        self.__logger = si_logger.get_logger(log_file_path=None, verbose_level=self.__verbose_level)
            
        self.__exec_date = datetime.utcnow()
        
        ####################
        #initialize folder structure and logger, read and store input parameters
        #main output dir
        try:
            self.__main_output_dir = get_and_del_param(dico_in, 'output_dir')
        except:
            raise MainArgError('output_dir parameter missing')
        self.__main_output_dir = os.path.abspath(self.__main_output_dir)
        try:
            assert os.path.isdir(self.__main_output_dir), 'main output directory must already exist'
        except:
            raise MainInputFileError('output_dir %s does not exist'%self.__main_output_dir)
        #create main_output_dir subdirectory structure
        self.__log_dir = os.path.join(self.__main_output_dir, 'logs')
        self.__data_dir = os.path.join(self.__main_output_dir, 'data')
        make_folder(self.__log_dir)
        make_folder(self.__data_dir)
        
        #add filehandler to logger (saves log to file)
        self.__log_file = os.path.join(self.__log_dir, 'csi_si_software.log')
        #save old log file if it exists
        if os.path.exists(self.__log_file):
            shutil.move(self.__log_file, self.__log_file + '_old_%s'%datetime2str(self.__exec_date))
        self.__logger = si_logger.LogUtil.add_file_handler(self.__logger, self.__log_file)
        
        try:
            #dias parameters and logger
            self.__dias_mode = False
            self.__dias_logger = None
            if 'dias_fsc_rlie_job_id' in dico_in:
                dias_fsc_rlie_job_id = get_and_del_param(dico_in, 'dias_fsc_rlie_job_id')
                if dias_fsc_rlie_job_id is not None:
                    self.__dias_mode = True
                    
                    #########
                    # old log used to communicate with orchestrator DB, no longer used
                    # ~ self.__dias_logger = si_logger.get_logger(verbose_level=1, fsc_rlie_job_id=dias_fsc_rlie_job_id)
                    #########
                    
                    self.__dias_logger = si_logger.get_logger(verbose_level=1)
        except Exception as ex:
            raise CodedException(str(ex), exitcode=exitcodes.dias_db_connexion_problem)
        
        
        #create temp dir = local working directory
        #temp dir
        try:
            temp_dir_loc = get_and_del_param(dico_in, 'temp_dir')
        except Exception as ex:
            raise MainArgError(str(ex))
        os.makedirs(temp_dir_loc, exist_ok=True)
        self.__main_temp_dir = tempfile.mkdtemp(dir=temp_dir_loc, prefix='fscrlie_')
        
        
        #check parameters remaining
        try:
            self.__dico = copy.deepcopy(dico_in)
            check_dict(self.__dico, ['copy_input_files_to_temp_dir', 'delete_temp_dir_on_error', 'delete_temp_dir_on_success', \
                'dem_dir', 'maja', 'lis', 'ice'], check_none=True)
                
            #nprocs, ram
            check_dict(self.__dico, ['nprocs', 'max_ram'], check_none=False)
            if self.__dico['nprocs'] is None:
                self.__dico['nprocs'] = cpu_count()
            set_gdal_otb_itk_env_vars(nprocs=self.__dico['nprocs'], max_ram=self.__dico['max_ram']) #setting itk otb and gdal env vars
            
            for el in ['copy_input_files_to_temp_dir', 'delete_temp_dir_on_error', 'delete_temp_dir_on_success']:
                assert isinstance(self.__dico[el], bool), 'expecting boolean for input parameter %s'%el
            check_dem_integrity(self.__dico['dem_dir'])
            self.__dico['tile_name'] = check_s2tile_name(self.__dico['dem_dir'].split('_')[-2])
            #maja
            self.__dico = check_input_maja_options(self.__dico)
            #lis
            check_dict(self.__dico['lis'], ['tree_cover_density', 'water_mask'], check_none=True)
            for el in ['tree_cover_density', 'water_mask']:
                assert self.__dico['lis'][el].split('_')[-1].split('.')[0] == self.__dico['tile_name'], \
                    'lis %s file name does not match tile name %s: %s'%(el, self.__dico['tile_name'], self.__dico['lis'][el])
                assert os.path.exists(self.__dico['lis'][el])
            check_dict(self.__dico['lis'], ['max_processing_time'], check_none=False)
            if self.__dico['lis']['max_processing_time'] is not None:
                self.__dico['lis']['max_processing_time'] = float(self.__dico['lis']['max_processing_time'])
            #ice
            check_dict(self.__dico['ice'], ['generate_product'], check_none=True)
            check_dict(self.__dico['ice'], ['river_shapefile', 'hrl_flags_file', 'max_processing_time'], check_none=False)
            assert isinstance(self.__dico['ice']['generate_product'], bool), 'expecting boolean for input parameter ice:generate_product'
            if self.__dico['ice']['generate_product']:
                assert self.__dico['ice']['river_shapefile'] is not None, \
                    'river shapefile must be specified in ice input parameters. If this is a tile without land water, then you must set generate_product to false'
                assert self.__dico['ice']['hrl_flags_file'] is not None, \
                    'hrl_flags_file must be specified in ice input parameters. If this is a tile without land water, then you must set generate_product to false'
                assert self.__dico['ice']['river_shapefile'].split('_')[-1].split('.')[0] == self.__dico['tile_name'], \
                    'ice river_shapefile file name does not match tile name %s: %s'%(self.__dico['tile_name'], self.__dico['ice']['river_shapefile'])
                assert self.__dico['ice']['hrl_flags_file'].split('_')[-1].split('.')[0] == self.__dico['tile_name'], \
                    'ice hrl_flags_file file name does not match tile name %s: %s'%(self.__dico['tile_name'], self.__dico['ice']['hrl_flags_file'])
                assert os.path.exists(self.__dico['ice']['river_shapefile'])
                assert os.path.exists(self.__dico['ice']['hrl_flags_file'])
            else:
                assert (self.__dico['ice']['river_shapefile'] is None) and (self.__dico['ice']['hrl_flags_file'] is  None), \
                    'river shapefile or hrl flags cannot be specified in ice input parameters if generate_product is set to false'
            if self.__dico['ice']['max_processing_time'] is not None:
                self.__dico['ice']['max_processing_time'] = float(self.__dico['ice']['max_processing_time'])
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
            #load template paths
            self.__templates = {el: os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', '%s_Metadata.xml'%(el.upper())) for el in ['fsc', 'rlie']}
            for el in self.__templates:
                assert os.path.exists(self.__templates[el]), 'template missing for %s product at path %s'%(el, self.__templates[el])
            ####################
            
            
            ####################
            #initialize processing tracking variables
            #processing status and product dict
            self.__product_to_task_correspondance = {'l2a': 'majal2a', 'fsc': 'lisfsc', 'rlie': 'icerlie'}
            self.__task_to_product_correspondance = {value: key for key, value in self.__product_to_task_correspondance.items()}
            
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
            if not self.__dico['maja']['save_output_l2a_file']:
                #delete l2a file from output dir and product list
                self.__clean_l2a_keep_metadata()
            if self.__dico['delete_temp_dir_on_success']: #delete temporary folder
                self.__delete_main_temporary_folder()
        elif CodedException('MAJA : Too cloudy !') == exit_value.__repr__():
            self.update_processing_status(new_value='exiting_cloudy')
            if self.__dico['delete_temp_dir_on_success'] or self.__dico['delete_temp_dir_on_error']: #delete temporary folder
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
                    self.__processing_statuses.append('fscrlieprocessingchain_restart')
                else:
                    self.__processing_statuses = ['fscrlieprocessingchain_start']
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
                assert len(new_product_keyval_tuple) == 2, 'expecting a key(l2a,fsc,rlie), value(valid path) tuple for new product insertion'
                assert new_product_keyval_tuple[0] in set(['l2a', 'fsc', 'rlie']) and os.path.exists(product_path), 'expecting a key(l2a,fsc,rlie), value(valid path) tuple for new product insertion'
                self.__product_dict[new_product_keyval_tuple[0]] = new_product_keyval_tuple[1]
                self.__write_product_dict_to_file()
                #update status of corresponding task with the productgenerated tag which is the final tag for a task (means job is finished and product was properly added)
                self.update_processing_status(new_value='%s_productgenerated'%self.__product_to_task_correspondance[new_product_keyval_tuple[0]])
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
        self.__product_information['product_mode_overide'] = self.__dico['maja']['product_mode_overide']
        self.__product_information['production_date'] = self.__exec_date
        
        
    def update_product_information_from_maja_output(self, l2a_file_work):
        """this routine must be triggered after maja L2A processing completion. It will add working l2a path 
        (can be different from product l2a path if the temporary:copy_input_files parameter is set to true) 
        and gather information from the L2A tile such as sensing date, tile coordinates, etc... used to fill
        FSC and RLIE products XML file."""
        self.__l2a_file_work = os.path.realpath(l2a_file_work)
        self.__product_information = update_product_information_from_l2a_file(self.__product_information, self.__l2a_file_work, temp_dir=self.__main_temp_dir)
        
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
        os.system('rm -Rf %s/*'%self.__main_temp_dir)
        
    def get_subtask_logger_writer(self, prefix=None):
        return SimpleWriteLinesToLoggerDebug(self.__logger, prefix=prefix)
        
    def get_product_path(self, key, check_exists=True):
        assert key in self.__product_to_task_correspondance, 'product key %s unknown'%key
        assert key in self.__product_dict, 'product %s does not exist'%key
        output_path = os.path.join(self.__data_dir, self.__product_dict[key])
        if check_exists:
            assert os.path.exists(output_path), 'product %s is in product dict but does not exist at path %s'%(key, output_path)
        return output_path
        
    def __clean_l2a_keep_metadata(self):
        l2a_path = self.get_product_path('l2a')
        clean_l2a_keep_metadata(l2a_path)
        
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
    def l2a_file_work_path(self):
        return self.__l2a_file_work
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



    
    

    
    
##########################################
#MAIN
def fsc_rlie_processing_chain(dico_in):
    

    #main processing information container object, handles logs
    #for logger to catch errors, this object must be opened within a context
    return_with_error_code = False
    if 'return_with_error_code' in dico_in:
        if dico_in['return_with_error_code']:
            return_with_error_code = True

    try:
        with Main_FSC_RLIE_Processing_Information(dico_in) as main_info:
            
            dico = main_info.input_parameters
            
            if ('fsc' in main_info.product_dict) and (('rlie' in main_info.product_dict) or (not dico['ice']['generate_product'])):
                main_info.logger_info('LIS and ICE processing already completed, skipping...')
                return
            
            ##########################################################################################
            #MAJA processing
            if 'l2a' not in main_info.product_dict:
                maja_l2a_processing(main_info)
            else:
                l2a_saved_file = main_info.get_product_path('l2a')
                main_info.logger_info('MAJA processing already completed, skipping...')
                if dico['copy_input_files_to_temp_dir']:
                    l2a_filename_work = os.path.join(main_info.main_temp_dir, os.path.filename(l2a_saved_file))
                    copy_original(l2a_saved_file, l2a_filename_work)
                else:
                    l2a_filename_work = l2a_saved_file
                main_info.update_product_information_from_maja_output(l2a_filename_work)
            
            ##########################################################################################
            #LIS
            if 'fsc' not in main_info.product_dict:
                lis_fsc_processing(main_info)
            else:
                main_info.logger_info('LIS processing already completed, skipping...')
                
                
            ##########################################################################################
            #ICE
            if dico['ice']['generate_product']:
                if 'rlie' not in main_info.product_dict:
                    ice_rlie_processing(main_info)
                else:
                    main_info.logger_info('ICE processing already completed, skipping...')
            else:
                main_info.logger_info('ICE processing not requested, skipping...')
    
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
        print(str(ex))
        if return_with_error_code:
            return exitcodes.default
        sys.exit(exitcodes.default)
    except:
        if return_with_error_code:
            return exitcodes.default
        print('Exiting with uncatchable error')
        raise
        sys.exit(exitcodes.default)
            
    return 0

    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to launch FSC and RLIE product generation on an S2 tile from level 1C Sentinel-2 product(s). Uses MAJA, LIS and ICE codes.')
        parser.add_argument("input_yaml_file", type=str, help='input yaml file')
        args = parser.parse_args()
            
        dico = load_yaml(args.input_yaml_file, env_vars=False)
        print('\n############## YAML INPUT ##############')
        print(dump_yaml(dico))
        print('########################################\n')
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        

    fsc_rlie_processing_chain(dico) #main function

