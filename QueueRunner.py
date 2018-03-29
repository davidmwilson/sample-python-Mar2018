'''
    this file contains QueueRunner class, used to run a JobQueue instance
'''
import os
import time
import logging

class QueueRunner(object):
    '''
        this class contains methods to run contained JobQueue
    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, job_queue, num_threads):
        '''
            constructor
        '''
        self._num_threads = num_threads
        self._job_queue = job_queue
        self._status = {}
        self.init_status()
        self._remaining_jobs = job_queue.len()
        self._total_jobs = self._remaining_jobs
        self._running_jobs = 0
        # track total exec time so we can track avg job times and project finish time etc
        self._total_exec_time = 0
        # ids of remaining jobs
        self._job_ids = sorted(self._job_queue.queue().keys(), reverse=False)
        self._start_time = 0
        self._end_time = 0

    def __repr__(self):
        '''
            override default print behavior
        '''
        return "QueueRunner: {} threads, {}/{} jobs remaining".format(
            self._num_threads, self._remaining_jobs, self._total_jobs)

    def print_detailed_status(self):
        '''
            inputs:  none
            outputs: none
            purpose: print summary queue stats plus per status
        '''
        logging.info("QueueRunner: %d threads, %d / %d jobs remaining in queue",
                     self._num_threads, self._remaining_jobs, self._total_jobs)
        for status in self._status.keys():
            logging.info("QueueRunner:%20s%8d", status, self._status[status])

    def complete_job(self, job_id, return_code):
        '''
            inputs:  job_id
                     return_code
            outputs: none
            purpose: complete job off queue
        '''
        if return_code == 0:
            status = "Complete"
        else:
            status = "Error"
        self.update_status('Running', '-')
        self.update_status(status, '+')
        self._running_jobs -= 1
        self._remaining_jobs -= 1
        self._job_queue[job_id].set_end(status, return_code)

    def check_job(self, job_id):
        '''
            inputs:  job_id
            outputs: none
            purpose: check child job for completion status
        '''
        # we use WNOHANG option for non-blocking check
        pid, return_code = os.waitpid(self._job_queue[job_id].pid, os.WNOHANG)
        if pid == 0:
            logging.info("check_job: job %s, pid: %d still running  ",
                         job_id, self._job_queue[job_id].pid)
        else:
            # shift return_code to get portion interested in
            #(leftmost 8 bits of 16 bit number on linux)
            return_code = return_code >> 8
            logging.info("check_job: job %s, pid: %d, completed with exitcode:%d  ",
                         job_id, pid, return_code)
            self.complete_job(job_id, return_code)

    def jobs_remaining(self):
        '''
            inputs:  none
            outputs: boolean
            purpose: indicate if jobs remaining to complete on queue
        '''
        return self._remaining_jobs > 0

    def check_jobs(self):
        '''
            inputs:  none
            outputs: none
            purpose: check status of spawned children
        '''
        logging.info("check_jobs: %d jobs in progress", self._running_jobs)
        for job_id in sorted(self._job_queue, reverse=False):
            if self._job_queue[job_id].status == 'Running':
                self.check_job(job_id)

    def init_status(self):
        '''
            inputs:  none
            outputs: none
            purpose: initialize internal dict of job status / count
        '''
        for status in ('Pending', 'Running', 'Complete', 'Error'):
            self._status[status] = 0

    def update_status(self, status, operation):
        '''
            inputs:  none
            outputs: none
            purpose: update internal dict of job status / count
        '''
        if status not in self._status.keys():
            self._status = 0

        if operation == "+":
            self._status[status] += 1
        elif operation == "-":
            self._status[status] -= 1
        else:
            logging.error("update_status: unknown operation %s for status %s", operation, status)

    def start_jobs(self):
        '''
            inputs:  none
            outputs: none
            purpose: start jobs from queue, up to specified num threads
        '''
        logging.info("start_jobs: %d jobs in progress", self._running_jobs)
        if self._running_jobs >= self._num_threads:
            logging.info("start_jobs: at maximum capacity of %d", self._num_threads)
            return
        for job_id in sorted(self._job_queue, reverse=False):
            if self._job_queue[job_id].status == 'Pending':
                logging.info("start_jobs: calling run_job for job %s", job_id)
                self._job_queue[job_id].run()
                self._running_jobs += 1
                self.update_status('Running', '+')
            if self._running_jobs >= self._num_threads:
                break

    def print_all_job_details(self):
        '''
            inputs:  none
            outputs: none
            purpose: print list of all jobs in queue
        '''
        for job_id in sorted(self._job_queue, reverse=False):
            self._job_queue[job_id].print_stats()

    def run(self):
        '''
            inputs:  none
            outputs: none
            purpose: run jobs from queue until complete
        '''
        logging.info("run_queue: %s: beginning execution of %d jobs",
                     self._job_queue.name, self._remaining_jobs)
        self._start_time = time.time()
        while self.jobs_remaining():
            self.print_detailed_status()
            self.check_jobs()
            self.start_jobs()
            time.sleep(3)
        self._end_time = time.time()
        exec_time = self._end_time - self._start_time
        self.print_detailed_status()
        logging.info("run_queue: %s: completed execution of %d jobs in %d seconds",
                     self._job_queue.name, self._total_jobs, exec_time)

