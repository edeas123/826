# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 23:58:14 2017
@author: Obaro Odiete
"""

""" setup the connection to the database """
def init():
    # retrieve configuration from file
    cfg = Config(file('config.txt'))
    
    # connect to database
    try:
        cnx = mysql.connector.connect(**cfg)
    except mysql.connector.Error as err:
        print(err)
    
    # get cursor for executing query
    cursor = cnx.cursor()

    return (cnx, cursor)


""" collective code to compute parameters of the aggregate framework
    such as the no_of_bins_x, no_of_bins_y and the 2d binned statistics required 
    for the heatmap and grid
"""
def aggr_framework(grid_size):

    no_of_bins_x = ((saskatoon_region_utm2[0] - saskatoon_region_utm1[0]) 
                    / grid_size) - 2
    no_of_bins_y = ((saskatoon_region_utm2[1] - saskatoon_region_utm1[1]) 
                        / grid_size) - 2
    
    # draw heat map using 2d histogram
    s = stats.binned_statistic_2d(x=saskatoon_utm[0], y=saskatoon_utm[1], 
                    values=None, statistic="count", 
                    bins= [int(no_of_bins_x), int(no_of_bins_y)])

    return (no_of_bins_x, no_of_bins_y, s)



""" operationalize dwell time """
def opr_dwell_time(no_of_bins_x, no_of_bins_y, s):
    # i am performing the binning again because i need to obtain a different 
    # representation for the binnumber, which i can use to locate the center 
    # for each bin
    print len(saskatoon_utm[0])
    sc = stats.binned_statistic_2d(x=saskatoon_utm[0], y=saskatoon_utm[1], 
                    values=None, statistic="count", 
                    bins= [int(no_of_bins_x), int(no_of_bins_y)], expand_binnumbers=True)
    
    #saskatoon_utm_grid
    x_center = (sc.x_edge[(sc.binnumber[0] - 1)] + sc.x_edge[(sc.binnumber[0])]) / 2
    y_center = (sc.y_edge[(sc.binnumber[1] - 1)] + sc.y_edge[(sc.binnumber[1])]) / 2
    
    # assign each records to a grid
    saskatoon_utm_grid = pd.DataFrame({'user_id': saskatoon_gps['user_id'], 
                             'record_time':saskatoon_gps['record_time'], 
                             'x': saskatoon_utm[0], 'y': saskatoon_utm[1], 
                             'bin':s[3], 'x_center': x_center, 'y_center': y_center},
                             columns=['user_id', 'record_time', 'x','y','bin', 'x_center', 'y_center'])
    
    # ensure the values are sorted by user_id and time
    saskatoon_utm_grid_sorted = saskatoon_utm_grid.sort_values(['user_id', 'record_time'])
    
    # get the index of each participants record for iteration purposes
    participants = [len(list(g)) for k, g in itertools.groupby(
            saskatoon_utm_grid_sorted['user_id'], lambda id: id)]
        
    # get the runlength for each participants in each grid
    # this corresponds to the dwell of each participant in that grid
    a = 0
    dwell_time = []
    for i in range(len(participants)):

        b = a + participants[i]
        tmp = [len(list(g)) for k, g in itertools.groupby(
                saskatoon_utm_grid_sorted['bin'][a:b], lambda bin: bin)]
        
        tmp_ser = pd.Series(tmp)
        
        tmp_rolling_start = tmp_ser.rolling(window=len(tmp), min_periods=1).sum()
        tmp_rolling_stop =  tmp_rolling_start - 1
        
        tmp_rolling_start.pop(len(tmp_rolling_start) -1)
        tmp_rolling_start = pd.Series([0]).append(tmp_rolling_start, ignore_index=True)
        
        tmp_t = (saskatoon_utm_grid_sorted['record_time'][a:b].values[tmp_rolling_stop] - 
                 saskatoon_utm_grid_sorted['record_time'][a:b].values[tmp_rolling_start]) / np.timedelta64(1, 's')
        
        a = b
        if not isinstance(tmp_t, np.ndarray):
            dwell_time.append(tmp_t)
            continue

        dwell_time.extend(tmp_t)
           
    # sort the dwell_length and count unique values, removing 0 dwell time
    dwell_time_sorted = pd.Series(sorted(dwell_time))
    dwell_time_sorted = dwell_time_sorted[dwell_time_sorted > 0]
    
    dwell = zip(*[(g[0], len(list(g[1]))) for g in itertools.groupby(dwell_time_sorted)])
    
    return (participants, saskatoon_utm_grid_sorted, dwell)


""" operationalize visit frequency """
def opr_visit_frequency(participants, saskatoon_utm_grid_sorted):
    
    # get the runlength for each participants in each grid
    # this corresponds to the visit count of each participant in that grid
    a = 0
    visit_length = []
    for i in range(len(participants)):
        b = a + participants[i]
        visit_length.extend([len(list(g)) for k, g in itertools.groupby(
                saskatoon_utm_grid_sorted['bin'][a:b], lambda bin: bin)])
        a = b
    
    # sort the visit_length and count unique values
    visit_length_sorted = sorted(visit_length)
    visit = zip(*[(g[0], len(list(g[1]))) for g in itertools.groupby(visit_length_sorted)])
    
    return visit


""" operationalize trip length """
def opr_trip_length(participants, saskatoon_utm_grid_sorted):
    
    # calculate the trip length for each particpants trip
    a = 0
    distance = pd.Series()
    for i in range(len(participants)):
        b = a + participants[i]
        tmp = np.sqrt(saskatoon_utm_grid_sorted['x_center'][a:b].rolling(window=2).apply(func=lambda x:(x[1]-x[0])**2) + 
                saskatoon_utm_grid_sorted['y_center'][a:b].rolling(window=2).apply(func=lambda y:(y[1]-y[0])**2))
        distance = distance.append(pd.Series(tmp))
        a = b    
        
    saskatoon_utm_grid_sorted['distance'] = distance.fillna(0)
    
    # narrow down to actual trips, i.e distance > 0
    saskatoon_trips = saskatoon_utm_grid_sorted[saskatoon_utm_grid_sorted['distance'] > 0]['distance']
    
    # distribution of trip distance
    saskatoon_trips_sorted = sorted(saskatoon_trips)
    trips = zip(*[(g[0], len(list(g[1]))) for g in itertools.groupby(saskatoon_trips_sorted)])
    
    return trips



""" .... main starts here ... """

# import required packages
import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
import itertools
import numpy as np
import random

from config import Config
from pyproj import Proj
from scipy import stats



""" STEP 1: Filtering """

(cnx, cursor) = init()
    
# query to calculate the total possible battery records
query = """
            select count(record_time) as ct 
            from battery 
            group by user_id 
            order by ct desc 
            limit 1
        """

# execute query
cursor.execute(query)
(max_battery_records,) = cursor.fetchone()

# query to get total number of participant
query = """
           select count(distinct(user_id))
           from battery
        """
# execute query
cursor.execute(query)
(number_of_participants,) = cursor.fetchone()

# query to remove all participants who have returned less than 50% 
# of total possible battery records.
cutoff = 0.5
query = ("select count(user_id) "
         "from ("
         "select user_id, count(record_time) as ct "
         "from battery " 
         "group by user_id "
         ") as user_battery_record_count "
         "where ct >= (%s * %s)")

# execute query
cursor.execute(query, (max_battery_records, cutoff))
(number_remaining_50,) = cursor.fetchone()

# query to remove all participants who have returned less than 75% 
# of total possible battery records.
cutoff = 0.75
query = ("select count(user_id) "
         "from ("
         "select user_id, count(record_time) as ct "
         "from battery " 
         "group by user_id "
         ") as user_battery_record_count "
         "where ct >= (%s * %s)")

# execute query
cursor.execute(query, (max_battery_records, cutoff))
(number_remaining_75,) = cursor.fetchone()

# how many participants did you eliminate in each of the 50% and 75% thresholds?
print "Number of participants: ", number_of_participants
print "Number of participants eliminated in 50% threshold: ", number_of_participants - number_remaining_50
print "Number of participants eliminated in 75% threshold: ", number_of_participants - number_remaining_75

# query to get all paritipant gps records
query = """select lat,lon from gps"""

# execute query
cursor.execute(query)

# all participants gps record
data = cursor.fetchall()
all_gps = pd.DataFrame(data, columns=['lat', 'lon'])

# number of gps records
number_of_gps_records = cursor.rowcount
print "Number of GPS records: ", number_of_gps_records

# second, remove all gps traces outside the city limits of greater saskatoon (52.058367, -106.7649138128), (52.214608, -106.52225318)
saskatoon_region_gps1 = (52.058367, -106.7649138128)
saskatoon_region_gps2 = (52.214608, -106.52225318)

query = ("select user_id, record_time, lat,lon "
         "from gps "
         "where (lat >= %s and lat <= %s) "
         "and (lon >= %s and lon <= %s)")

# execute query
cursor.execute(query, (saskatoon_region_gps1[0], saskatoon_region_gps2[0], saskatoon_region_gps1[1], saskatoon_region_gps2[1]))

# all participants gps record in greater saskatoon
data = cursor.fetchall()
saskatoon_gps = pd.DataFrame(data, columns=['user_id', 'record_time', 'lat', 'lon'])

# how many gps records did you eliminate? 
number_of_used_gps_records = cursor.rowcount
print "Number of GPS records eliminated: ", number_of_gps_records - \
                                            number_of_used_gps_records
       
# plot all gps for all participants. 
plt.plot(all_gps['lat'], all_gps['lon'], '.')
plt.ylabel('longitude')
plt.xlabel('latitude')

# are there any suspicious locations? 
print "Are there suspicious location?", "Yes"

# how would you have filtered them if you had not filtered for saskatoon? 
print "How would you have filtered them if you had not filtered for saskatoon?", "See attached report"




""" STEP 2: Aggregation Framework """

# initialize PROJ
p = Proj(init="epsg:32613")

# convert gps coordinates to utm coordinates
saskatoon_utm = p(saskatoon_gps['lon'].values, saskatoon_gps['lat'].values)

# convert gp coordinate for the greater saskatoon region to utm coordinates
saskatoon_region_utm1 = p(saskatoon_region_gps1[1], saskatoon_region_gps1[0])
saskatoon_region_utm2 = p(saskatoon_region_gps2[1], saskatoon_region_gps2[0])

   
# set grid size and calculate no of bins on both dimensions
grid_size = 200
(no_of_bins_x, no_of_bins_y, s_200) = aggr_framework(grid_size)

# plot the heatmap
fig = plt.figure()
plt.imshow(s_200[0])
plt.show()

# plot the heatmap imposed on a base map of the location
# plot done seperately using jupyter notebook



""" STEP 3 """
""" Operationalize dwell time """

# call function to operationalize dwell time
(participants_200, saskatoon_utm_grid_sorted_200, dwell_200) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_200)

# plot the log-log plot of the dwell-length

plt.ylabel('count (#)')
plt.title('Distribution of aggregate metrics for 200m grid cells for all study participants')
plt.loglog(dwell_200[0], dwell_200[1], label='dwell time(s)')

""" Operationalize visit frequency """

# call function to operationalize visit frequency
visit_200 = opr_visit_frequency(participants_200, saskatoon_utm_grid_sorted_200)

# plot the log-log plot of the visit-length
plt.loglog(visit_200[0], visit_200[1], label='visit frequency(count)')


""" Operationalize trip length """

trips_200 = opr_trip_length(participants_200, saskatoon_utm_grid_sorted_200)

# plot the log-log plot of the trip-length
plt.loglog(trips_200[0], trips_200[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


""" Step 4"""

""" Changing the resolution: grid_size = 100 """

grid_size = 100
(no_of_bins_x, no_of_bins_y, s_100) = aggr_framework(grid_size)

fig = plt.figure()
plt.imshow(s_100[0])
plt.show()

(participants_100, saskatoon_utm_grid_sorted_100, dwell_100) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_100)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metrics for 100m grid cells for all study participants')
plt.loglog(dwell_100[0], dwell_100[1], label='dwell time(s)')

""" Operationalize visit frequency """

# call function to operationalize visit time
visit_100 = opr_visit_frequency(participants_100, saskatoon_utm_grid_sorted_100)

# plot the log-log plot of the visit-length
plt.loglog(visit_100[0], visit_100[1], label='visit frequency(count)')

""" Operationalize trip length """

trips_100 = opr_trip_length(participants_100, saskatoon_utm_grid_sorted_100)

# plot the log-log plot of the trip-length
plt.loglog(trips_100[0], trips_100[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


""" Changing the resolution: grid_size = 400 """

grid_size = 400
(no_of_bins_x, no_of_bins_y, s_400) = aggr_framework(grid_size)

fig = plt.figure()
plt.imshow(s_400[0])
plt.show()

(participants_400, saskatoon_utm_grid_sorted_400, dwell_400) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_400)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metric for 400m grid cells for all study participants')
plt.loglog(dwell_400[0], dwell_400[1], label='dwell time(s)')


""" Operationalize visit frequency """

# call function to operationalize visit time
visit_400 = opr_visit_frequency(participants_400, saskatoon_utm_grid_sorted_400)

# plot the log-log plot of the visit-length
plt.loglog(visit_400[0], visit_400[1], label='visit frequency(count)')

""" Operationalize trip length """

trips_400 = opr_trip_length(participants_400, saskatoon_utm_grid_sorted_400)

# plot the log-log plot of the trip-length
plt.loglog(trips_400[0], trips_400[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()



""" Changing the resolution: grid_size = 1600 """

grid_size = 1600
(no_of_bins_x, no_of_bins_y, s_1600) = aggr_framework(grid_size)


fig = plt.figure()
plt.title("1600m heat map of UTM coordinates for all study participants")
plt.imshow(s_1600[0])
plt.show()

(participants_1600, saskatoon_utm_grid_sorted_1600, dwell_1600) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_1600)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metric for 1600m grid cells for all study participants')
plt.loglog(dwell_1600[0], dwell_1600[1], label='dwell time(s)')


""" Operationalize visit frequency """

# call function to operationalize visit frequency
visit_1600 = opr_visit_frequency(participants_1600, saskatoon_utm_grid_sorted_1600)

# plot the log-log plot of the visit-length
plt.loglog(visit_1600[0], visit_1600[1], label='visit frequency(count)')


""" Operationalize trip length """

trips_1600 = opr_trip_length(participants_1600, saskatoon_utm_grid_sorted_1600)

# plot the log-log plot of the trip-length
plt.loglog(trips_1600[0], trips_1600[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Trip length (m)')
plt.title('Distribution of Trip Length for different sized grid cells for all study participants')
plt.loglog(trips_100[0], trips_100[1], label='100m Grid cells')
plt.loglog(trips_200[0], trips_200[1], label='200m Grid cells')
plt.loglog(trips_400[0], trips_400[1], label='400m Grid cells')
plt.loglog(trips_1600[0], trips_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()

# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Visit Frequency (count)')
plt.title('Distribution of Visit Frequency for different sized grid cells for all study participants')
plt.loglog(visit_100[0], visit_100[1], label='100m Grid cells')
plt.loglog(visit_200[0], visit_200[1], label='200m Grid cells')
plt.loglog(visit_400[0], visit_400[1], label='400m Grid cells')
plt.loglog(visit_1600[0], visit_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()

# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Dwell Time (s)')
plt.title('Distribution of Dwell Time for different sized grid cells for all study participants')
plt.loglog(dwell_100[0], dwell_100[1], label='100m Grid cells')
plt.loglog(dwell_200[0], dwell_200[1], label='200m Grid cells')
plt.loglog(dwell_400[0], dwell_400[1], label='400m Grid cells')
plt.loglog(dwell_1600[0], dwell_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()











""" using random participants """
# run sql query and retrieve fresh data

query = """select distinct(user_id) from gps"""

# execute query
cursor.execute(query)

# all participants gps record
user_ids = cursor.fetchall()

# sample to get random 3
[(id1,), (id2,), (id3,)] = random.sample(user_ids, 3)

query = ("select user_id, record_time, lat,lon "
         "from gps "
         "where (lat >= %s and lat <= %s) "
         "and (lon >= %s and lon <= %s) "
         "and (user_id in (%s, %s, %s))")

# execute query
cursor.execute(query, (saskatoon_region_gps1[0], saskatoon_region_gps2[0], 
                       saskatoon_region_gps1[1], saskatoon_region_gps2[1],
                       id1, id2, id3))

# all participants gps record in greater saskatoon
data = cursor.fetchall()
saskatoon_gps = pd.DataFrame(data, columns=['user_id', 'record_time', 'lat', 'lon'])

# convert gps coordinates to utm coordinates
saskatoon_utm = p(saskatoon_gps['lon'].values, saskatoon_gps['lat'].values)

# set grid size and calculate no of bins on both dimensions
grid_size = 200
(no_of_bins_x, no_of_bins_y, s_200) = aggr_framework(grid_size)

# plot the heatmap
fig = plt.figure()
plt.imshow(s_200[0])
plt.show()

# plot the heatmap imposed on a base map of the location
# plot done seperately using jupyter notebook



""" STEP 3 """
""" Operationalize dwell time """

# call function to operationalize dwell time
(participants_200, saskatoon_utm_grid_sorted_200, dwell_200) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_200)

# plot the log-log plot of the dwell-length

plt.ylabel('count (#)')
plt.title('Distribution of aggregate metrics for 200m grid cells for three random participants')
plt.loglog(dwell_200[0], dwell_200[1], label='dwell time(s)')

""" Operationalize visit frequency """

# call function to operationalize visit frequency
visit_200 = opr_visit_frequency(participants_200, saskatoon_utm_grid_sorted_200)

# plot the log-log plot of the visit-length
plt.loglog(visit_200[0], visit_200[1], label='visit frequency(count)')


""" Operationalize trip length """

trips_200 = opr_trip_length(participants_200, saskatoon_utm_grid_sorted_200)

# plot the log-log plot of the trip-length
plt.loglog(trips_200[0], trips_200[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


""" Step 4"""

""" Changing the resolution: grid_size = 100 """

grid_size = 100
(no_of_bins_x, no_of_bins_y, s_100) = aggr_framework(grid_size)

fig = plt.figure()
plt.imshow(s_100[0])
plt.show()

(participants_100, saskatoon_utm_grid_sorted_100, dwell_100) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_100)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metrics for 100m grid cells for three random participants')
plt.loglog(dwell_100[0], dwell_100[1], label='dwell time(s)')

""" Operationalize visit frequency """

# call function to operationalize visit time
visit_100 = opr_visit_frequency(participants_100, saskatoon_utm_grid_sorted_100)

# plot the log-log plot of the visit-length
plt.loglog(visit_100[0], visit_100[1], label='visit frequency(count)')

""" Operationalize trip length """

trips_100 = opr_trip_length(participants_100, saskatoon_utm_grid_sorted_100)

# plot the log-log plot of the trip-length
plt.loglog(trips_100[0], trips_100[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


""" Changing the resolution: grid_size = 400 """

grid_size = 400
(no_of_bins_x, no_of_bins_y, s_400) = aggr_framework(grid_size)

fig = plt.figure()
plt.imshow(s_400[0])
plt.show()

(participants_400, saskatoon_utm_grid_sorted_400, dwell_400) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_400)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metric for 400m grid cells for three random participants')
plt.loglog(dwell_400[0], dwell_400[1], label='dwell time(s)')


""" Operationalize visit frequency """

# call function to operationalize visit time
visit_400 = opr_visit_frequency(participants_400, saskatoon_utm_grid_sorted_400)

# plot the log-log plot of the visit-length
plt.loglog(visit_400[0], visit_400[1], label='visit frequency(count)')

""" Operationalize trip length """

trips_400 = opr_trip_length(participants_400, saskatoon_utm_grid_sorted_400)

# plot the log-log plot of the trip-length
plt.loglog(trips_400[0], trips_400[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()



""" Changing the resolution: grid_size = 1600 """

grid_size = 1600
(no_of_bins_x, no_of_bins_y, s_1600) = aggr_framework(grid_size)


fig = plt.figure()
plt.title("1600m heat map of UTM coordinates for three random participants")
plt.imshow(s_1600[0])
plt.show()

(participants_1600, saskatoon_utm_grid_sorted_1600, dwell_1600) = opr_dwell_time(no_of_bins_x, no_of_bins_y, s_1600)

# plot the log-log plot of the dwell-length
plt.ylabel('count (#)')
plt.title('Distribution of aggregate metric for 1600m grid cells for three random participants')
plt.loglog(dwell_1600[0], dwell_1600[1], label='dwell time(s)')

""" Operationalize visit frequency """

# call function to operationalize visit frequency
visit_1600 = opr_visit_frequency(participants_1600, saskatoon_utm_grid_sorted_1600)

# plot the log-log plot of the visit-length
plt.loglog(visit_1600[0], visit_1600[1], label='visit frequency(count)')

""" Operationalize trip length """

trips_1600 = opr_trip_length(participants_1600, saskatoon_utm_grid_sorted_1600)

# plot the log-log plot of the trip-length
plt.loglog(trips_1600[0], trips_1600[1], label='trip length(m)')

# show plot
plt.legend()
plt.show()


# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Trip length (m)')
plt.title('Distribution of Trip Length for different sized grid cells for three random participants')
plt.loglog(trips_100[0], trips_100[1], label='100m Grid cells')
plt.loglog(trips_200[0], trips_200[1], label='200m Grid cells')
plt.loglog(trips_400[0], trips_400[1], label='400m Grid cells')
plt.loglog(trips_1600[0], trips_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()

# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Visit Frequency (count)')
plt.title('Distribution of Visit Frequency for different sized grid cells for three random participants')
plt.loglog(visit_100[0], visit_100[1], label='100m Grid cells')
plt.loglog(visit_200[0], visit_200[1], label='200m Grid cells')
plt.loglog(visit_400[0], visit_400[1], label='400m Grid cells')
plt.loglog(visit_1600[0], visit_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()

# plot differently - grouping measures but different resolutions
plt.ylabel('count (#)')
plt.xlabel('Dwell Time (s)')
plt.title('Distribution of Dwell Time for different sized grid cells for three random participants')
plt.loglog(dwell_100[0], dwell_100[1], label='100m Grid cells')
plt.loglog(dwell_200[0], dwell_200[1], label='200m Grid cells')
plt.loglog(dwell_400[0], dwell_400[1], label='400m Grid cells')
plt.loglog(dwell_1600[0], dwell_1600[1], label='1600m Grid Cells')
plt.legend()
plt.show()











""" cleanups """
# close cursor
cursor.close()

# close database connection
cnx.close()
