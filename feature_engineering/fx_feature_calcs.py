## Functions for calculating FX features uses as factor signal inputs.
## Includes functions to calculate market based variables such as carry, etc

import pandas as pd
import numpy as np

def calc_fx_carry(spot_t, forward_t, tenor_days):
    """
    Calculate FX carry based on spot rate, forward rate, and tenor in days.  Assumes USDXXX exchange rate.
    
    Parameters:
    spot_t (float): The current spot exchange rate.
    forward_t (float): The forward exchange rate for the given tenor.
    tenor_days (int): The number of days until the forward contract matures.
    
    Returns:
    float: The calculated FX carry.
    """
    carry = np.log(forward_t / spot_t) * (360 / tenor_days)
    return carry
