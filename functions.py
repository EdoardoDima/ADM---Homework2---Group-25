import pandas as pd
import numpy as np
import seaborn as sb
import scipy as sp
import matplotlib as mp
from datetime import datetime
import math
from tabulate import tabulate
import time

pd.options.mode.chained_assignment = None

def positive_reviews(df):
    rec_pos = df[df.recommended==True]
    return rec_pos.groupby('app_name').review_id.count().sort_values(ascending=False).head(50).plot.bar(figsize = (18, 7), title = 'Games with the most positive reviews', colormap = 'Pastel2', xlabel = "App's name", ylabel = 'Number of positive reviews', rot = 90)

def negative_reviews(df):
    rec_neg = df[df.recommended==False]
    rec_neg.groupby('app_name').review_id.count().sort_values(ascending=False).head(50).plot.bar(figsize = (18, 7), title = 'Games with the most negative reviews', colormap = 'Pastel1', xlabel = "App's name", ylabel = 'Number of negative reviews')


def dateparse_nosecs(time_in_secs):
    x = pd.to_datetime(time_in_secs, unit = 's')
    return x.dt.floor('T') #the datetime object resulting from this parsing will have "00" as a value for seconds

def dateparse_secs(time_in_secs):
    return pd.to_datetime(time_in_secs, unit = 's') #this other dateparser takes into consideration seconds too


def not_updated(df, c, u):
    count_not_updated = 0
    for row in range(len(df)):
        if c[row] == u[row]:
            count_not_updated += 1
    not_updated = count_not_updated / len(df)
    #not_updated_rounded = round(not_updated, 2)

    print("The percentage of reviews that don't get updated is %s " % (round(not_updated, 2)) + '%')


def purchased_free_ratio(df):
    purchased = df[(df.steam_purchase==True) & (df.received_for_free==False)]
    purchased2 = purchased.groupby('steam_purchase')
    gifted = df[(df.received_for_free==True) & (df.steam_purchase==False)]
    gifted2 = gifted.groupby('received_for_free')
    comprato_da_terzi = df[(df.steam_purchase==False) & (df.received_for_free==False)]
    comprato_da_terzi2 = comprato_da_terzi.groupby('steam_purchase')

    comprati = int(comprato_da_terzi2.size()) + int(purchased2.size())
    regalati = int(gifted2.size())

    dummy = [comprati, regalati]

    return mp.pyplot.pie(dummy, radius = 2.5, shadow = True, autopct = '%.1f', labels = ('Purchased', 'Received for free'), startangle = 350, explode=[1 for value in range(0, len(dummy))], textprops={'fontsize': 14})



def time_interval(df, time_list): 
    interval_counts = {}
    
    for i in range(0, len(time_list), 2):

        if len(time_list) % 2 != 0:
            print('Please insert an even number of elements in the list')
            
        else:
            a = (datetime.strptime((time_list[i]), '%H:%M:%S')).time()
            b = (datetime.strptime((time_list[i+1]), '%H:%M:%S')).time()         
            interval = df[(df.timestamp_created_secs >= a) & (df.timestamp_created_secs <= b)]            
            count = len(interval.timestamp_created_secs)
            interval_counts['%s - %s' %(a, b)] = count

    data_items = interval_counts.items()
    data_list = list(data_items)
    df = pd.DataFrame(data_list)
   
    
    return df.plot.bar(x = 0,
    figsize = (18, 7), title = 'Number of reviews by time interval', colormap = 'Spectral', 
    xlabel = "Time interval", ylabel = 'Number of reviews (in millions)', rot = 20, legend = '')


# input : df:name: a dataframe, size: the size of a chunk
def top3(df_name, size):
    # define a list to memorize partial results
    dfs = list()
    # for each chunk, get the groups for each language and their size, 
    # then add to the partial results to be combined 
    for chunk in np.array_split(df_name, size): 
        d = chunk[['language']].groupby('language', sort=False).size().reset_index(name='counts')
        dfs.append(d)
    # merge the partial solution to get the overall solution
    df_merged = pd.concat(dfs)
    # group again to get the groups and sum the partial sizes, then sort 
    d = df_merged.groupby('language', sort=False).agg("sum").reset_index(). \
        sort_values(by="counts", ascending=False)
    # return a list containing the top3 languages
    return d['language'].tolist()[0:3]


# take as input the result of the function above
def filter_top3(df_name, size):
    langs = top3(df_name, size)
    df_l, t = list(), 0
    # for each chunk, add to a counter the number of lines to get the dimension of the df
    # filter the rows in which language is set to one of the values in the list
    for chunk in np.array_split(df_name, size): 
        t += chunk.shape[0]
        d = chunk[(chunk['language'].isin(langs))]
        df_l.append(d)
    # combine the results
    f_top3 = pd.concat(df_l)
    # return the filtered df and the percentage of reviews written in the top3 languages
    return f_top3, (f_top3.shape[0]/t)*100


def stats_top3(df_name, size):
    top3_df, p = filter_top3(df_name, size)
    v = top3_df.shape[0]
    vh, vf = 0, 0
    #count the reviews voted as helpful (and funny)
    for chunk in np.array_split(top3_df, size):
        vh += chunk[['votes_helpful']][chunk.votes_helpful > 0].shape[0]
        vf += chunk[['votes_funny']][chunk.votes_funny > 0].shape[0]
    # return 3 numbers 
    return p, vh / v * 100, vf / v * 100 


def top_steamers(df_name, size):
    partials = list()
    #n = 0
    for chunk in np.array_split(df_name, size): #pd.read_csv(df_name, chunksize=size):
        #n += 1
        chunk = chunk[['author.steamid', 'author.num_reviews', 'votes_helpful', 'votes_funny', 'comment_count']]
        chunk['p_sum'] = chunk.loc[:, ['votes_helpful', 'votes_funny', 'comment_count']].sum(axis=1) #nuova colonna
        chunk = chunk.groupby('author.steamid', sort=False) \
            .agg(local_rev=('author.num_reviews', 'count'), p_sum=('p_sum', 'sum')).reset_index()
        partials.append(chunk)
        '''if n > 8:
            break'''
    d = pd.concat(partials)
    chunk = chunk.groupby('author.steamid', sort = False) \
        .agg(local=('local_rev', 'count'), p_sum=('p_sum', 'sum')).reset_index()
    #print(chunk)
    #d['pop'] = d['p_sum'] / d['local_rev']
    d['pop'] = d['p_sum'] * (d['local_rev'] / 100)
    d = d.sort_values(by="pop", ascending=False)
    #print(d)
    return d['author.steamid'].tolist()[0:10], d[['author.steamid', 'pop']].head(10).plot.bar(figsize = (18, 7), xlabel = "Author's Steam ID", title = 'Most popular reviewers', ylabel = '"Popular" score', colormap = 'Pastel1', x = 'author.steamid', y = 'pop', rot = 30, legend = '')


def top_steamer_stats(df_name, size, top_steamer):

    partials, all_apps = list(), list()
    
    for chunk in np.array_split(df_name, size): #pd.read_csv(df_name, chunksize=size):
        
        try:
            p = chunk[chunk['author.steamid'] == top_steamer]
            partials.append(p)
            all_apps += p['app_name'].to_list()
        except Exception as e:
            continue
        
    df = pd.concat(partials)
    # number of apps that the top-steamer reviewed
    n_apps = df.shape[0]
    # number of apps the steamer received for free
    free_counts = df[df.received_for_free == True].shape[0]
    free_stats_pos = df[(df.received_for_free == True) & (df.recommended == True)]
    free_stats_neg = df[(df.received_for_free == True) & (df.recommended == False)]
    #free_stats2 = free_stats.groupby('recommended').size()
    # number of apps the steamer purchased
    purchase_counts = df[df.steam_purchase == True].shape[0]
    purchase_stats_pos = df[(df.steam_purchase == True) & (df.recommended == True)]
    purchase_stats_neg = df[(df.steam_purchase == True) & (df.recommended == False)]
    #.recommended.value_counts()
    return set(all_apps), \
           free_counts, (free_counts / n_apps) * 100, \
           purchase_counts, (purchase_counts / n_apps) * 100, \
           len(free_stats_pos), len(purchase_stats_pos), len(free_stats_neg), len(free_stats_pos)

def average_time_to_update(df, c, u, a, twentyone):
    days = 0
    seconds = 0
    for row in range(len(df)):
        diff = u[row] - c[row]
        days += diff.days
        seconds += diff.seconds
    av_d = days // twentyone #this variable here is just the number of rows in the dataframe
    sec = ((days / twentyone) - av_d)*24*3600
    av_s = (seconds / twentyone) + sec

    print('An user lets pass in average %s days and %s seconds to update a review' %(av_d, int(av_s)))


def top_three_updaters(df, c, u, a):
    d = {}
    for row in range(len(df)):
        if c[row] != u[row]:
            if a[row] in d:
                d[a[row]] = d[a[row]] + 1
            else:
                d[a[row]] = 1
    d1 = sorted(d, key=d.get, reverse=True)
    #result = [i for i in d1[:3]]
    result = {}
    for i in d1[:3]:
        result[i] = d[i]                               
        #result.append(d[i])

    data_items = result.items()
    data_list = list(data_items)
    result_df = pd.DataFrame(data_list)

    result_df.plot.bar(figsize=(10, 7), x=0, y=1, xlabel = "Author's ID", ylabel = 'Nr. of updates', rot=30, legend = '', colormap = 'Pastel1')


def prob_wg(df, wg, twentyone):
    count = 0
    for row in range(len(df)):
        if wg[row ] >= 0.5:
            count += 1
    prob1 = count / twentyone
    prob1_rounded = round(prob1, 2)
    return prob1, prob1_rounded
    

def prob_fun(df, wg, funny, twentyone):
    count1 = 0
    count2 = 0
    for row in range(len(df)):
        if ( wg[row]) > 0.5 and (funny[row]) >= 1:
            count1 += 1
        elif wg[row] > 0.5:
            count2 += 1
    prob2 = count1 / count2
    prob2_rounded = round(prob2, 2)
    return prob2_rounded

    


def indipendence(df, wg, funny, twentyone, probability_wg):
    count1 = 0
    for row in range(len(df)):
        if funny[row] >= 1:
            count1 += 1
    prob3 = count1 / twentyone

    count2 = 0
    for row in range(len(df)):
        if ((funny[row]) >=1) and ((wg[row]) >= 0.5):
            count2 += 1
    prob4 = count2 / twentyone

    return prob4 == prob3 * probability_wg 





    #if prob4 == prob3 * probability_wg:
     #   print('The probability that "a review has at least one vote as funny" is independent from the "probability that a review has a Weighted Vote Score equal or bigger than 0.5')

    #else:
     #       print('The probability that "a review has at least one vote as funny" is not independent from the "probability that a review has a Weighted Vote Score equal or bigger than 0.5')