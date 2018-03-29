'''
    example usage of Job / JobQueue / QueueRunner
'''
import logging
from datetime import date
from Job import Job
from JobQueue import JobQueue
from QueueRunner import QueueRunner

def main():
    '''
        main example to construct / run queue of jobs
    '''
    # create queue
    job_queue = JobQueue("test")
    # start and end date
    start_date = date(2018, 1, 1)
    end_date = date(2018, 1, 11)
    # this would be a useful example of what to run
    cmdline = "loadDate -db TAQ -date {yyyymmdd}"
    # this is a less useful example, but serves for example purposes on linux at least
    cmdline = "echo Hello, today is {yyyymmdd}"
    num_threads = 4
    # overrides

    # this call will populate based on below params
    # can also just call add_job over and over as needed
    logging.basicConfig(filename='main.log', level=logging.INFO,
                        format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    job_queue.populate(start_date, end_date, cmdline)

    # here's how to add one manually
    my_job = Job('99999999', 'echo Manually added job')
    job_queue.add(my_job)
    # create queue runner
    queue_runner = QueueRunner(job_queue, num_threads)
    print("beginning run of queue")
    # run queue until complete
    queue_runner.run()
    # log stats at end
    queue_runner.print_all_job_details()
    print("completed run of queue")

# when this is called on own, invoke main
if __name__ == "__main__":
    main()
