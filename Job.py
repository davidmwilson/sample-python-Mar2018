''' define Job class '''
import time
import os
import logging
from subprocess import call

class Job(object):
    ''' this class encapsulates a Job which contains command line as well as stats like run time '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, job_id, command_line):
        self.command_line = command_line
        self.job_id = job_id
        self.status = "Pending"
        self.pid = 0
        self.return_code = 0
        self.start_time = 0
        self.end_time = 0
        self.exec_time = 0

    def __repr__(self):
        '''
            override default print behavior
        '''
        return "Job %s,status:%s:%s"%(self.job_id, self.status, self.command_line)

    def set_start(self, status, pid):
        '''
            name:     set_start
            input:    status
                      process id
            output:   none
            purpose:  set member vars to denote start of job running
        '''
        self.status = status
        self.pid = pid
        self.start_time = int(time.time())

    def print_stats(self):
        '''
            name:     print_stats
            input:    none
            output:   none
            purpose:  print details about job - status, exec time etc
        '''
        logging.info("Job: %-20sStatus: %-15sExecTime(secs) :%-12d", self.job_id, self.status, self.exec_time)

    def set_end(self, status, return_code):
        '''
            name:     set_end
            input:    status
                      process id
            output:   none
            purpose:  set member vars to denote start of job running
        '''
        self.status = status
        self.return_code = return_code
        self.end_time = int(time.time())
        self.exec_time = self.end_time - self.start_time

    def run(self):
        '''
            name:     run
            input:    none
            output:   none
            purpose:  fork sub process to run defined job
        '''
        newpid = os.fork()
        # if pid returned is zero, you are the child process
        if newpid == 0:
            # child
            logging.info("Job.run:starting child with cmd %s", self.command_line)
            return_code = 0
            try:
                return_code = call(self.command_line, shell=True)
            except OSError as my_exception:
                logging.error("Job.run: error %d running child process %s",
                              my_exception.errno, self.command_line)
                exit(-1)
            ## left in for testing purposes only
            ## if pid is even, return 0 otherwise return 1
            ##if os.getpid()%2!=0:
            ##    return_code=1
            #
            if return_code == 0:
                logging.info("Job.run:completed child process for job %s, return code %d",
                             self.job_id, return_code)
            else:
                logging.error("Job.run:child process for job %s failed with return code %d",
                              self.job_id, return_code)

            # finish with expected return code
            exit(return_code)
        else:
            # parent
            logging.info("Job.run:started child job %s with pid %d", self.job_id, newpid)
            self.set_start("Running", newpid)

