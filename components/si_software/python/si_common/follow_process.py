#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from si_common.common_functions import *

from threading import Thread, Event
from queue import Queue, Empty
import multiprocessing
import signal



#############################################


    
def dump_execution_dict_to_directory(execution_dict, output_directory):
    os.makedirs(output_directory, exist_ok=True)
    with open('%s/status.txt'%output_directory, mode='w') as ds:
        ds.write('%s\n'%('\n'.join(['%s: %s'%(key, execution_dict[key]) for key in ['exceeded_time', 'execution_time', 'returncode']])))
    for key in ['stdout', 'stderr']:
        with open('%s/%s.txt'%(output_directory, key), mode='w') as ds:
            ds.write('%s\n'%('\n'.join(execution_dict[key])))



def get_from_queue(queue_in, timeout=None):
    try:
        if timeout is not None:
            out = queue_in.get(block=True,timeout=timeout)
        else:
            out = queue_in.get(block=False)
    except Empty:
        out = None
    return out


def get_all_from_queue(queue_in):
    returned_values = []
    while(True):
        returned_value = get_from_queue(queue_in)
        if returned_value is None:
            break
        returned_values.append(returned_value)
    return returned_values
        
        


class ThreadCheckTimer(object):
    
    def __init__(self, dt_min=1.e-2, dt_max=1.):
        self.time_start = time.time()
        for el in [dt_min, dt_max]:
            try:
                if not (el > 0.):
                    raise InnerArgError('Cannot start ParallelThreadOptimizedCheckTimer with dt_min < 0. or dt_max < 0.')
            except:
                raise InnerArgError('dt_min and dt_max must be float in ParallelThreadOptimizedCheckTimer __init__')
        self.dt_min = dt_min
        self.dt_max = dt_max
        
    def get_dt(self):
        return min(self.dt_max, max(self.dt_min, time.time()-self.time_start))

        
def stoppable_thread(exitqueue, target_function, args):
    #must be launched in a thread !!! otherwise it has no sense !!!
    #starts target_function in a parallel thread and then checks for exit orders
    thread_loc = Thread(target=target_function, args=args)
    thread_loc.start()
    try:
        tim = ThreadCheckTimer()
        while(True):
            if not thread_loc.is_alive():
                return
            exitorder = get_from_queue(exitqueue)
            if exitorder == 'exit':
                return
            time.sleep(tim.get_dt())
    finally:
        thread_loc.join()
    
    
def enqueue_output_reader(reader, linequeue):
    try:
        for line in iter(reader.readline, b''):
            line = bytes2string(line).replace('\n','')
            linequeue.put(line)
    finally:
        reader.close()
    

    
class ProcessReadDaemon(object):
    
    def __init__(self, reader):
        
        self.reader = reader
        self.linequeue = Queue()
        self.exitqueue = Queue()
        self.thread = None
        self.active = False
        
    def start(self):
        self.thread = Thread(target=stoppable_thread, args=(self.exitqueue, enqueue_output_reader, (self.reader, self.linequeue)))
        self.thread.start()
        self.active = True
        
    def stop(self):
        if self.thread.is_alive():
            time.sleep(0.1)
            self.exitqueue.put('exit')
        self.thread.join()
            
    def get_output_lines(self):
        return get_all_from_queue(self.linequeue)




class SimpleTaskManager(multiprocessing.Process):
    """Launches parallel tasks (command_list), monitors execution time (can shutdown tasks if too long), and catches stderr and stdout live"""
    
    def __init__(self, identifier, cmd, stdout_queue, stderr_queue, return_queue, exit_queue, maxtime_seconds=None, verbose=0):
        if sys.version_info.major == 3:
            super().__init__()
        else:
            super(SimpleTaskManager, self).__init__()
        self.verbose = verbose
        self.identifier = identifier
        self.cmd = cmd
        self.stdout_queue = stdout_queue
        self.stderr_queue = stderr_queue
        self.return_queue = return_queue
        self.exit_queue = exit_queue
        self.maxtime_seconds = maxtime_seconds
        self.forced_exit = False
        self.running_task = None
        self.stdout_daemon, self.stderr_daemon = None, None
        self.return_dict = None

        
    def start_read_daemons(self):
        if self.running_task is None:
            raise InnerArgError('cannot start stdout/stderr read daemons if running task does not exist')
        self.stdout_daemon = ProcessReadDaemon(self.running_task.stdout)
        self.stderr_daemon = ProcessReadDaemon(self.running_task.stderr)
        self.stdout_daemon.start()
        self.stderr_daemon.start()
        if any([not el for el in [self.stdout_daemon.active, self.stderr_daemon.active]]):
            raise InnerArgError('read daemons not started properly')
        
    
    def update_read_daemons(self):
        for line in self.stdout_daemon.get_output_lines():
            self.stdout_queue.put(line)
        for line in self.stderr_daemon.get_output_lines():
            self.stderr_queue.put(line)
        
        
    def stop_read_daemons(self):
        self.update_read_daemons()
        self.stdout_daemon.stop()
        self.stderr_daemon.stop()        
        
                
    def kill_running_task(self):
        if self.running_task.poll() is not None:
            return
        self.forced_exit = True
        #kill task softly
        try:
            os.killpg(os.getpgid(self.running_task.pid), signal.SIGTERM)
        except:
            pass
        time.sleep(1)
        #kill task the hard way if still persistent after 1 second
        try:
            os.killpg(os.getpgid(self.running_task.pid), signal.SIGKILL)
        except:
            pass
            

    def run(self):
        
        start_time = time.time()
        self.running_task = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, preexec_fn=os.setsid)
        self.start_read_daemons()
        
        exceeded_time = False
        try:
            while(True):
                self.update_read_daemons()
                if self.running_task.poll() is not None:
                    break
                exitorders = get_all_from_queue(self.exit_queue)
                if 'exit' in exitorders:
                    break
                if self.running_task.poll() is not None:
                    break
                if self.maxtime_seconds is not None:
                    if time.time()-start_time > self.maxtime_seconds:
                        print('exceeeded allocated time, exiting process')
                        exceeded_time = True
                        break
                time.sleep(1)
        except Exception as exe:
            print(str(exe))
        finally:
            self.kill_running_task()
            self.stop_read_daemons()
            self.return_queue.put({self.identifier: {'returncode': self.running_task.returncode, 'execution_time': time.time()-start_time, \
                'exceeded_time': exceeded_time, 'forced_exit': self.forced_exit}})
        

        
class SimplePrintLines(object):
    def __init__(self, prefix=None, print_date=True):
        self.prefix = prefix
    def write_lines(self, lines):
        date_tag = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S%Z')
        for line in lines:
            print('%s (%s): %s'%(self.prefix, date_tag, line))
        
class SimpleWriteLinesToLog(object):
    def __init__(self, descriptor):
        self.descriptor = descriptor
    def write_lines(self, lines):
        self.descriptor.write('%s\n'%('\n'.join(lines)))
        

class SimpleWriteLinesToLoggerDebug():
    def __init__(self, logger, prefix=None):
        self.__logger = logger
        self.__prefix = prefix
        if self.__prefix is None:
            self.__prefix = ''
    def write_lines(self, lines):
        for line in lines:
            self.__logger.debug('%s: %s'%(self.__prefix, line))
    
    
def execute_write_tasks(write_objects, lines):
    for write_object in write_objects:
        write_object.write_lines(lines)


############################################################################
    
def execute_commands(cmd_dict, maxtime_seconds=None, scan_dt=1, verbose=1):
    
    exit_src = None
    
    start_time = time.time()
    
    tasks = dict()
    stdout_queues = {key: multiprocessing.Queue(0) for key in cmd_dict}
    stderr_queues = {key: multiprocessing.Queue(0) for key in cmd_dict}
    return_queue = multiprocessing.Queue(0)
    exit_queues = {key: multiprocessing.Queue(0) for key in cmd_dict}
    for key in cmd_dict:
        for el in ['cmd']:
            if el not in cmd_dict[key]:
                raise InnerArgError('key %s missing from cmd_dict'%el)
        for el in ['stdout_write_objects', 'stderr_write_objects', 'stdout_logfiles', 'stderr_logfiles']:
            if el not in cmd_dict[key]:
                cmd_dict[key][el] = []
            if not isinstance(cmd_dict[key][el], list):
                cmd_dict[key][el] = [cmd_dict[key][el]]
        tasks[key] = SimpleTaskManager(key, cmd_dict[key]['cmd'], stdout_queues[key], stderr_queues[key], return_queue, exit_queues[key], \
            maxtime_seconds=maxtime_seconds, verbose=verbose)
        
    execution_dict = {key: {'returncode': None, 'stdout': [], 'stderr': [], 'execution_time': None, 'exceeded_time': None, 'forced_exit': None} for key in cmd_dict}
    stdout_write_tasks = dict()
    stderr_write_tasks = dict()
    logfile_descriptors = dict()
    for key in cmd_dict.keys():
        stdout_write_tasks[key] = []
        stderr_write_tasks[key] = []
        if verbose > 0:
            stdout_write_tasks[key].append(SimplePrintLines(prefix=key + ' stdout'))
            stderr_write_tasks[key].append(SimplePrintLines(prefix=key + ' stderr'))
        
        stdout_write_tasks[key] += cmd_dict[key]['stdout_write_objects']
        stderr_write_tasks[key] += cmd_dict[key]['stderr_write_objects']
            
        for logfile in cmd_dict[key]['stdout_logfiles']:
            if logfile not in logfile_descriptors:
                logfile_descriptors[logfile] = open(logfile, mode='w', buffering=1)
            stdout_write_tasks[key].append(SimpleWriteLinesToLog(logfile_descriptors[logfile]))
        for logfile in cmd_dict[key]['stderr_logfiles']:
            if logfile not in logfile_descriptors:
                logfile_descriptors[logfile] = open(logfile, mode='w', buffering=1)
            stderr_write_tasks[key].append(SimpleWriteLinesToLog(logfile_descriptors[logfile]))
    

    keys_check = set(cmd_dict.keys())
    try:
        for key in cmd_dict:
            print('Launching command:\n%s'%cmd_dict[key]['cmd'])
            tasks[key].start()
        while True:
            
            #look for returned jobs
            returned_values = get_all_from_queue(return_queue)
            for returned_value in returned_values:
                for key in returned_value.keys():
                    for subkey in returned_value[key].keys():
                        execution_dict[key][subkey] = returned_value[key][subkey]
                    tasks[key].terminate()
                    tasks[key].join()
                    del tasks[key]
                    
            #update stout/stderr dicts
            for key in keys_check:
                stdout_newlines = get_all_from_queue(stdout_queues[key])
                stderr_newlines = get_all_from_queue(stderr_queues[key])
                #function calls (contains writes to log, stdout, etc...)
                execute_write_tasks(stdout_write_tasks[key], stdout_newlines)
                execute_write_tasks(stderr_write_tasks[key], stderr_newlines)
                #fill execution_dict
                execution_dict[key]['stdout'] += stdout_newlines
                execution_dict[key]['stderr'] += stderr_newlines
                
            for returned_value in returned_values:
                keys_check = keys_check - set(returned_value.keys())
            if any([execution_dict[el]['forced_exit'] for el in execution_dict]):
                raise InterruptedError('subprocess exit')
            if len(keys_check) == 0:
                break
            if not returned_values:
                time.sleep(scan_dt)
    
    finally:

        #close all remaining jobs
        still_running = set(tasks.keys())
        for key in still_running:
            exit_queues[key].put('exit')
        for key in still_running:
            tasks[key].terminate()
            tasks[key].join()
            del tasks[key]
            
        #look for returned jobs
        still_running_manager_not_returned = still_running
        returned_values = get_all_from_queue(return_queue)
        for returned_value in returned_values:
            for key in returned_value.keys():
                still_running_manager_not_returned = still_running_manager_not_returned - set([key])
                for subkey in returned_value[key].keys():
                    execution_dict[key][subkey] = returned_value[key][subkey]

        #update stout/stderr dicts
        for key in keys_check:
            stdout_newlines = get_all_from_queue(stdout_queues[key])
            stderr_newlines = get_all_from_queue(stderr_queues[key])
            if key in still_running:
                stderr_newlines.append('%s interrupted'%key)
            if key in still_running_manager_not_returned:
                stderr_newlines.append('%s did not return after interruption'%key)
            #function calls (contains writes to log, stdout, etc...)
            execute_write_tasks(stdout_write_tasks[key], stdout_newlines)
            execute_write_tasks(stderr_write_tasks[key], stderr_newlines)
            #fill execution_dict
            execution_dict[key]['stdout'] += stdout_newlines
            execution_dict[key]['stderr'] += stderr_newlines
                    
        for key in logfile_descriptors:
            logfile_descriptors[key].close()

            
    return execution_dict
    
    
        
if __name__ == "__main__":
    
    #test
    logfile_out = 'test_out.log'
    logfile_err = 'test_err.log'
    execution_dict = execute_commands({'dummy': {'cmd': ['./dummy_process.py'], 'stdout_logfiles': ['test_out_dummy.log'], 'stderr_logfiles': ['test_err_dummy.log']}, \
        'weird': {'cmd': ['./weird_process.py'], 'stdout_logfiles': ['test_out_weird.log'], 'stderr_logfiles': ['test_err_weird.log']}}, \
        maxtime_seconds=1000, scan_dt=1, verbose=1)
    print(execution_dict)



