from mathematicians import simple_get 
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import threading

def read_data(name, variables):
    """
    Reads the csv file under the 'name' and places it into a dataframe
    Parameters: name of csv file
    Returns: dataframe
    """
    df = pd.read_csv(name, usecols = variables)
    return df

def plot_data(df, cols, df_name):
    """
    Plots time against different values in 'cols'
    Parameters: dataset, columns, y axis label
    Returns: none
    """
    ax = plt.gca()
    for i in range(len(cols)):
        plt.plot(df['decimal'], df[cols[i]])
    plt.xlabel('Time (yrs)')
    plt.ylabel(df_name) 
    plt.show()
    return

def combine_data(df1, df2):
    """
    Combines the co2 and ch4 data into one dataframe and lines them up based on the same month, year pair, eliminates the redundant year and month column    
    Parameters: dataframe 1, dataframe 2
    Returns: combined dataframe
    """
    df = pd.merge(df1, df2, on='decimal', suffixes=('_co2', '_ch4'))
    df = df.drop(['year_ch4', 'month_ch4'], axis = 1) 
    #df['toff'] = pd.read_csv('toff_output2.csv')
    return df

def process_data(df):
    """
    Divides the 'average_ch4' and 'trend_ch4' values by 1000 to convert from ppb to ppm
    Parameter: dataframe
    Return: modified dataframe
    """
    df['average_ch4'] = df.loc[:, 'average_ch4']/1000
    df['trend_ch4'] = df.loc[:, 'trend_ch4']/1000
    df = round(df, 4)
    return df

def get_IR(co2, ch4, toff):
    """
    Calculates the Upward IR Heat Flux using at the specified 'co2' level, 'ch4' level, and with the 'toff'
    Parameters: co2 leve, ch4 level, temperature offset
    Returns: integrated radation
    """
    coeff = 3.14*10**4
    url = 'http://climatemodels.uchicago.edu/cgi-bin/modtran/modtran.cgi?pco2={co2_level}&ch4={ch4_level}&trop_o3=0&strat_o3=1&Toffset={toff_level}&h2otscaled=0&h2orat=1&scalefreon=1&model=2&icld=0&altitude=70&i_obs=180&i_save=0'
    url = url.format(co2_level = co2, ch4_level=ch4, toff_level = toff)
    response = simple_get(url)
    response = str(response)
    IR = coeff*float(re.search('INTEGRATED RADIANCE =  (.*?) WATTS', response).group(1)) 
    return round(IR,3)  

def get_offset(pre_IR, curr_IR, co2, ch4):
    """
    Use binary search to find the correct temperature offset that can return IR to pre industrial levels. This will give us the predicted temperature change.
    Parameters: pre industrial IR flux levels, current industrial IR flux levels pre-adjustment, co2 level, ch4 level, initial temperature offset
    Returns: temperature offest that brings curr_IR to pre_IR    
    """ 
    #may not require rounding!
    lower = 0 
    upper = lower+2
    #preform a binary search between 0 and upper
    while(curr_IR != pre_IR): 
        curr_IR = round(get_IR(co2, ch4, (lower+upper)/2), 3)
        if (curr_IR > pre_IR): 
           upper = round((lower+upper)/2, 3) 
        elif (curr_IR < pre_IR): 
           lower = round((lower+upper)/2, 3) 
    print((lower+upper)/2)
    #print((lower+upper)/2, file=f)
    return (lower+upper)/2

def add_columns(df, i, j):
    #df['IR'] = [get_IR(list(df.loc[i,['average_co2','average_ch4']])[0],list(df.loc[i,['average_co2','average_ch4']])[1],0) for i in range(df.shape[0])]
    pre_co2 = 277.7
    pre_ch4 = 0.7233 
    pre_IR = get_IR(pre_co2, pre_ch4, 0)

    while i < j:
        gas_con = list(df.loc[i,['average_co2','average_ch4']])
        df.loc[i, 'IR'] = get_IR(gas_con[0], gas_con[1], 0)
        df.loc[i, 'toff'] = get_offset(pre_IR, df.loc[i, 'IR'], gas_con[0], gas_con[1])   
        i+=1
    
    """
    f = open('toff_output.csv', 'w')
    for i in range(df.shape[0]):
        gas_con = list(df.loc[i,['average_co2','average_ch4']])
        df.loc[i, 'IR'] = get_IR(gas_con[0], gas_con[1], 0)
        df.loc[i, 'toff'] = get_offset(pre_IR, df.loc[i, 'IR'], gas_con[0], gas_con[1])   
    f.close()
    """
 
def main():
    variables = ['year', 'month', 'decimal', 'average', 'trend']
    df_co2 = read_data('co2_mm_gl.csv', variables)
    df_ch4 = read_data('ch4_mm_gl.csv', variables)

    plot_data(df_co2, ['average','trend'], 'CO2 Level (ppm)')

    plot_data(df_ch4, ['average','trend'], 'CH4 Level (ppb)')

    
    df = combine_data(df_co2, df_ch4)
    df = process_data(df)

    numThreads = 6
    i = 0
    step = int(df.shape[0]/numThreads)
    threads = []
    while (i < numThreads):
        if (i == numThreads-1):
           threads.append(threading.Thread(target=add_columns, args=(df, i * step, df.shape[0])))
        else:
            threads.append(threading.Thread(target=add_columns, args=(df, i * step, (i + 1)*step)))

        threads[i].start()
        i+=1

    for t in threads:
        t.join()
     
    f = open('toff_output.csv', 'w')
    print(df['toff'], file=f)
    f.close()

    plot_data(df, ['IR'], 'IR W/m^-2')

    plot_data(df, ['toff'], 'Temperature Offset (' + u'\u00B0'+"C)")

main()
