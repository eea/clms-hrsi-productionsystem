#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#######################################################################
#  This code has been developped by Magellium SAS
#
#  Licensing:
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>
#######################################################################



""" 
    Author: remi.jugier@magellium.fr
    Example:
    jobs= [Job(identity='shell_example', shell_command='ls'), Job(identity='function_example', function='function_name', args=[], keyargs={})]
    dico = simple_parallel_run(jobs, nprocs)
"""

import sys
assert sys.version_info.major==3, 'python 3 version required'
import time
import multiprocessing
from queue import Empty as queue_empty
import subprocess
import traceback



def simple_parallel_run(jobs, nprocs):
    """runs non dependant jobs in parallel
    Returns a dict of identity:(succeeded,output) tuple"""
    jg = JobGroup()
    if isinstance(jobs, list):
        for job in jobs:
            jg.add(SequentialJobs(jobs=[job]))
    else:
        raise Exception('jobs must be a list of Job objects')
    jgl = JobGroupList(jobgroups=[jg])
    with Processes(nprocs) as process_object:
        jgl.run(process_object)
    return jgl.get_return_dict()
        
        
        

########################################
class JobGroupList(object):
    """List of JobGroups that should be run sequentially"""
    def __init__(self, jobgroups=None):
        if jobgroups is None:
            self.jobgroups = []
        else:
            self.jobgroups = jobgroups
        
    def add(self, jobgroup):
        self.jobgroups.append(jobgroup)
        
    def check_unicity(self):
        identities = []
        for jobgroup in self.jobgroups:
            for seq_joblist in jobgroup.sequential_joblists:
                for job in seq_joblist.jobs:
                    if job.identity is not None:
                        identities.append(job.identity)
        if len(identities) != len(list(set(identities))):
            raise Exception('Job identifiers not unique')
            
    def run(self, process_object):
        self.check_unicity()
        for jobgroup in self.jobgroups:
            process_object.run_jobgroup(jobgroup)
            
    def get_return_dict(self):
        return_dict = dict()
        for jobgroup in self.jobgroups:
            for seq_joblist in jobgroup.sequential_joblists:
                for job in seq_joblist.jobs:
                    if job.identity is not None:
                        return_dict[job.identity] = {'output': job.output, 'succeeded': job.job_succeeded, 'run_info': job.run_info}
        return return_dict
        
        
        
class JobGroup(object):
    """List of sequential job list that can be run in parallel"""
    def __init__(self, sequential_joblists=None):
        if sequential_joblists is None:
            self.sequential_joblists = []
        else:
            self.sequential_joblists = sequential_joblists
        
    def add(self, sequential_joblist):
        self.sequential_joblists.append(sequential_joblist)
        
        
        
class SequentialJobs(object):
    """List of sequential jobs"""
    def __init__(self, jobs=None):
        if jobs is None:
            self.jobs = []
        else:
            self.jobs = jobs
            
    def add(self, job):
        self.jobs.append(job)

        
        
        
class Job(object):
    """Job"""
    def __init__(self, identity=None, shell_command=None, function=None, args=None, keyargs=None):
        self.identity = identity
        self.shell_command = shell_command
        self.is_shell = self.shell_command is not None
        if self.is_shell:
            if not ((function is None) and (args is None) and (keyargs is None)):
                raise Exception('Job can be either shell command or function but not both')
        else:
            self.args = args
            self.keyargs = keyargs
            if type(function) == str:
                if function not in globals():
                    raise Exception('function not found in current variables: %s'%function)
                self.function = globals()[function]
            else:
                self.function = function
        self.job_started = False
        self.job_succeeded = False
        self.run_info = 'Initialized'
        self.error = None
        self.output = None
        
    def run(self):
        self.job_started = True
        self.run_info = 'Running'
        if self.is_shell:
            output = subprocess.call(self.shell_command, shell=True)
            if output == 0:
                self.job_succeeded = True
                self.run_info = 'Job succeeded'
            else:
                self.error = output
                self.run_info = 'Job failed: returned code %s'%output
        else:
            try:
                if (self.args is not None) and (self.keyargs is not None):
                    self.output = self.function(*self.args, **self.keyargs)
                elif self.args is not None:
                    self.output = self.function(*self.args)
                elif self.keyargs is not None:
                    self.output = self.function(**self.keyargs)
                else:
                    self.output = self.function()
                self.job_succeeded = True
                self.run_info = 'Job succeeded'
            except:
                error = sys.exc_info()
                self.error = '%s\n%s'%(error[1], '\n'.join(traceback.format_tb(error[2])))
                self.run_info = 'Job failed: %s'%self.error

        
    
        
        
class Processes(object):
    """Creates processes, loads tasks in a workQueue and gets result in a resul queue"""
    def __init__(self, nprocesses):
        self.nprocesses = nprocesses
        self.workQueue = multiprocessing.Queue(0)
        self.returnQueue = multiprocessing.Queue(0)
        self.processes = []
        
    def __enter__(self):
        for processID in range(self.nprocesses):
            process = Process(processID, self.workQueue, self.returnQueue)
            process.start()
            self.processes.append(process)
        return self
            
    def __exit__(self, exc_type, exc_value, traceback):
        for process in self.processes:
            while(process.is_alive()):
                self.workQueue.put(None)
            process.join()
        time.sleep(0.01)

    def run_jobgroup(self, jobgroup):

        for isequential_joblist, sequential_joblist in enumerate(jobgroup.sequential_joblists):
            self.workQueue.put([isequential_joblist, sequential_joblist])
        returned = []
        njobs = len(jobgroup.sequential_joblists)
        all_returned = njobs == 0
        while not all_returned:
            try:
                isequential_joblist, sequential_joblist = self.returnQueue.get(block=True, timeout=0.01)
                jobgroup.sequential_joblists[isequential_joblist] = sequential_joblist
                returned.append(isequential_joblist)
            except queue_empty:
                if len(returned) == njobs:
                    all_returned = set(returned) == set(range(njobs))
            except:
                raise
        time.sleep(0.01)




class Process(multiprocessing.Process):
    
    def __init__(self, processID, workQueue, returnQueue):
        super(Process, self).__init__()
        self.processID = processID
        self.workQueue = workQueue
        self.returnQueue = returnQueue
        
    def run(self):
        while True:
            try:
                tuple_seq = self.workQueue.get(block=True,timeout=0.01)
                if tuple_seq is None:
                    break
                iseq_jobs, seq_jobs = tuple_seq
                for job in seq_jobs.jobs:
                    job.run()
                self.returnQueue.put([iseq_jobs, seq_jobs])
            except queue_empty:
                pass
            except:
                raise
        
        








