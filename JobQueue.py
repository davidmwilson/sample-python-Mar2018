'''
    this file contains defintion of JobQueue class
'''
from Job import Job
#from Helper import date_range, format_cmd_line
import Helper as Helper

class JobQueue(object):
    '''
        This object represents Queue of Job objects
    '''

    def __init__(self, name):
        '''
            default constructor
        '''
        #self._helper = Helper()
        self.name = name
        # ideally we'd like people not to access queue directly
	# so signal nicely by putting _ at front
        self._queue = {}

    def __repr__(self):
        '''
            override default print behavior
        '''
        return "Queue:%s,%d jobs"%(self.name, len(self._queue))

    def __getitem__(self, key):
        '''
            implement queue index accessor
        '''
        return self._queue[key]

    def __iter__(self):
        '''
            implement queue iterator
        '''
        for job in self._queue:
            yield job

    def len(self):
        '''
            length method to access inner private queue
        '''
        return len(self._queue)

    def add(self, job):
        '''
            Name:    add
            Input:   job
            Output:  none
            Purpose: add job to queue
        '''
        self._queue[job.job_id] = job

    def queue(self):
        '''
            accessor for queue
        '''
        return self._queue

    def populate(self, start_date, end_date, cmdline):
        '''
            Name:    populate
            Input:   start date
                     end date
                     command line template
            Output:  final command line
            Purpose: generate list of jobs based on start/end/cmdline
        '''

        for day in Helper.date_range(start_date, end_date):
            job_cmdline = Helper.format_cmd_line(cmdline, day)
            # for future iterations, generate dictionary of translations per day
            # so don't do this twice. mea culpa
            yyyymmdd = day.strftime("%Y%m%d")
            job = Job(yyyymmdd, job_cmdline)
            self.add(job)

