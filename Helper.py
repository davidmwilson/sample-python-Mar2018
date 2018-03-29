'''
    This file contains helper functions
'''
from datetime import timedelta

def date_range(start_date, end_date):
    '''
        Name:    dateRange
        Input:   start date
                 end date
        Output:  list of dates
        Purpose: generate list of dates between start and end
    '''
    # now return list of dates
    for date_element in range(int((end_date-start_date).days)):
        yield start_date+timedelta(date_element)

def format_cmd_line(cmdline, day):
    '''
        Name:    formatCmdLine
        Input:   raw command line
                 date
        Output:  final command line
        Purpose: take a cmd line template and substitute in dates
             according to handful of predefined formats
    '''
    # yyyymmdd only currently supported format
    cmdline = cmdline.replace("{yyyymmdd}", day.strftime("%Y%m%d"))
    # more below...
    # ... sometime
    return cmdline

