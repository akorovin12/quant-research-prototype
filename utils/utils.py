import datetime
import numpy as np

def find_third_wed(month:int,year:int)->datetime:
    """
    Finds the 3rd Wed of a given month and year.
    """
    wed=2
    first_day = datetime.date(year,month,1)
    first_day_idx = first_day.weekday()
    first_wed = first_day + datetime.timedelta(days=(wed-first_day.weekday()) % 7)
    third_wed = first_wed + datetime.timedelta(weeks=2)
    return third_wed

def next_imm_date(start_dt:datetime,offset:int=0)->datetime:
    """
    Calculate the next IMM date after a given start date, with an optional offset.

    IMM dates are the third Wednesday of March, June, September, and December.

    Parameters:
        start_dt (datetime): The starting date to calculate from.
        offset (int): The number of IMM dates to offset. Default is 0 (next IMM date).

    Returns:
        datetime: The calculated IMM date.
    """
    imm_months = [3, 6, 9, 12]
    curr_year = start_dt.year
    curr_month = start_dt.month

    #counter=0
    prev_imm = None
    while True:
        #print(f"counter:{counter};curr_month:{curr_month};curr_year:{curr_year}")
        next_imm_month = min([m for m in imm_months if m>=curr_month])
        imm_date = find_third_wed(next_imm_month,curr_year)
        #print(f"next_imm_month:{next_imm_month};imm_date:{imm_date}")
       
        if imm_date>start_dt:
            break
        else:
            prev_imm = imm_date
            if curr_month==12:
                curr_month=1
                curr_year+=1
            else:
                curr_month+=1
        #counter+=1

    # Now we have our next IMM date.  If no offset, we're done.
    if offset==0:
        return imm_date
    #What if we have an offset?
    # special case when start_dt is an imm date
    elif offset==-1 and prev_imm!=None:
        return prev_imm
    else:
        num_months_offset = offset*3
        num_years_add = int(num_months_offset/(15 - next_imm_month))
        idx = imm_months.index(next_imm_month)
        idx_new = (offset % 4) + idx
        return find_third_wed(imm_months[idx_new],imm_date.year+num_years_add)