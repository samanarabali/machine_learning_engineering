# Project 1: Query Project

In this project, I am proposing a few options along with their analysis to increase the ridership for Lyft Bay Wheels. There are a range of frequent offers available like Single Ride, Monthly Membership, Annual Membership, Bike Share for All, Access Pass, Corporate Membership, etc. In this project, I will explore the 5 most popular trips that I would call "commuter trips", my recommendations for offers and some general recomendations to increase the ridership along with justifications and analysis. I am using the following static tables in the dataset san_francisco which is publicly available on Google Cloud Platform.

- bikeshare_stations
- bikeshare_status
- bikeshare_trips


### Part 1 - Querying Data with BigQuery

The first thing we are going to explore is the size of this dataset. To answer this question, I break it down into a few questions. First: 
- What is the total number of unique trips? 

The following SQL query from the bikeshare_trips shows 983,648 unique trips. Running this quesry with and without `distinct()` gives the same result which implies that the trip_ids are quique.

```sql
SELECT count(distinct(trip_id)) AS Numer_of_Trips
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
```
```
Row	   Number_of_Trips	
1	     983648
```
- What is the earliest start date and time and latest end date and time for a trip?

I used the min() and max() functions to find the earliest and latest trips recorded in this dataset.

```sql
SELECT 
min(start_date) AS earliest_start_date,
max(end_date) AS latest_start_date
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
```
```
Row	earliest_start_date	             latest_start_date	
1	2013-08-29 09:08:00 UTC          2016-08-31 23:48:00 UTC
```
- How many bikes are there? 

To answer this question, I simply query the count of distinct `bike_number` below. It looks like a total of 700 bikes are there.   
```sql
SELECT count(distinct (bike_number)) AS Number_of_Bikes
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
```
```
Row	  Number_of_Bikes	
1            700	
```

- Regions with the highest number of stations?

Looks like Lyft has the highest number of bike stations installed in San Francisco with 37. 

```sql
SELECT landmark, count(station_id) as number_of_stations 
FROM `bigquery-public-data.san_francisco.bikeshare_stations`
group by landmark
order by number_of_stations desc 
LIMIT 5
```

```
Row	    landmark	      number_of_stations
1	    San Francisco           37
2	    San Jose                18
3	    Redwood City            7
4	    Mountain View           7
5	    Palo Alto               5
```

In this section, I will make up at least 3 questions and answer them using the Bay Area Bike Share Trips Data. 

Q1: What are top 5 most popular trips? 

The most popular trips are those start/end station pairs that have the highest numbers of count in the dataset.

```sql
SELECT count(trip_id) as count_id, start_station_name, end_station_name 
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
group by start_station_name, end_station_name 
order by count_id desc
```

```
Row	count_id	start_station_name	                       end_station_name	
1	9150	    Harry Bridges Plaza (Ferry Building)       Embarcadero at Sansome	
2	8508	    San Francisco Caltrain 2 (330 Townsend)    Townsend at 7th	
3	7620	    2nd at Townsend                            Harry Bridges Plaza (Ferry Building)	
4	6888	    Harry Bridges Plaza (Ferry Building)       2nd at Townsend	
5	6874	    Embarcadero at Sansome                     Steuart at Market	

```
Q2: What is the longest trip?

To address this question I first find the maximum duration trip using `max()` and then in another query I find the information of the trip where its duration matches with the maximum duration. Looks like trip_id 568474 from "South Van Ness at Market" to "2nd at Folsom" last about 6 months. This is a 108 mile trip and assuming the recorded dates are correct, it cannot be a single trip.

```sql
SELECT  trip_id, start_station_name, end_station_name, start_date, end_date, duration_sec/3600 AS duration_hour
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
where duration_sec = (
    select max(duration_sec)
    from `bigquery-public-data.san_francisco.bikeshare_trips`
)
```

```
Row	trip_id	start_station_name	     end_station_name	start_date	             end_date	              duration_hour	
1   568474  South Van Ness at Market 2nd at Folsom      2014-12-06 21:59:00 UTC  2015-06-24 20:18:00 UTC  4797.333333333333
```

Q3: Find the total number of roundtrips?

A roundtrip is a trip where the start and end stations are the same. This condition is applied under "where" statement. The total number of roundtrips is 32047.  

```sql
SELECT  count(trip_id) as Total_Roundtrips
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
where start_station_name = end_station_name
```

```
Row	 Total_Roundtrips	
1	 32047
```

Q4: Find the top most popular bike stations for a roundtrip?

Groupped by start and end station name and orderred by trip_id count gives the most favorable stations for a roundtrip. Embarcadero at Sansome is the most popular one with a total of 2,866 roundrips.

```sql
SELECT  count(trip_id) as trip_id_count, start_station_name, end_station_name
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
where start_station_name = end_station_name
group by start_station_name, end_station_name
order by trip_id_count desc
limit 5
```

```
Row	trip_id_count	start_station_name                            end_station_name
1	2866	        Embarcadero at Sansome                        Embarcadero at Sansome
2	2364	        Harry Bridges Plaza (Ferry Building)          Harry Bridges Plaza (Ferry Building)
3	1184	        University and Emerson                        University and Emerson
4	944	            Market at 4th                                 Market at 4th
5	911	            Steuart at Market                             Steuart at Market
```

Q5: Top tree most popular station pairs within a region?

I used `landmark` from "stations" table to get the region and two JOIN have been performed on the station_id for both start and end stations and the `trip_id` are counted.  

```sql
SELECT count(trip_id) as number_of_trips, 
    start_station_name, 
    station.landmark, 
    end_station_name, 
    station1.landmark 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` trip
join `bigquery-public-data.san_francisco.bikeshare_stations` station on trip.start_station_id = station.station_id
join `bigquery-public-data.san_francisco.bikeshare_stations` station1 on trip.end_station_id = station1.station_id
where station.landmark = station1.landmark 
group by start_station_name,start_station_name, station.landmark, end_station_name, station1.landmark
order by number_of_trips desc
limit 3
```

```
Row	number_of_trips	start_station_name	         landmark	               end_station_name	             landmark_1
1	9150	       Harry Bridges Plaza (Ferry Building)	San Francisco	   Embarcadero at Sansome        San Francisco
2	8508	       San Francisco Caltrain 2 (330 Townsend)	San Francisco	Townsend at 7th	             San Francisco
3	7620	       2nd at Townsend	San Francisco	          Harry Bridges Plaza (Ferry Building)       San Francisco

```


### Part 2 - Querying data from the BigQuery CLI

In this section, we repeat the first three queries using the commandline interface.   

- What's the size of this dataset? (i.e., how many trips)

```sql
bq query --use_legacy_sql=false '
    SELECT count(distinct(trip_id)) AS Numer_of_Trips
    FROM
       `bigquery-public-data.san_francisco.bikeshare_trips`'
```
```
+----------------+
| Numer_of_Trips |
+----------------+
|         983648 |
```
- What is the earliest start date and time and latest end date and time for a trip? 

I used the min() and max() functions to find the earliest and latest trips recorded in this dataset.

```sql
bq query --use_legacy_sql=false '
SELECT 
min(start_date) AS earliest_start_date,
max(end_date) AS latest_start_date
FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
```
```
| earliest_start_date |  latest_start_date  |
+---------------------+---------------------+
| 2013-08-29 09:08:00 | 2016-08-31 23:48:00 |
```
- How many bikes are there? 

To answer this question, I simply query the count of distinct `bike_number` below. It looks like a total of 700 bikes are there.   
```sql
bq query --use_legacy_sql=false '
SELECT count(distinct (bike_number)) AS Number_of_Bikes
FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
```
```
| Number_of_Bikes |
+-----------------+
|             700 |
```

- How many trips are in the morning vs in the afternoon?

The following two queries count the number of trips in the morning and afternoon. I used `Time()` to extract the time of the trip. The trips which both start and end hours are in the morning count as "morning trips" and those with both start and end hours are in the afteroon count as "afternoon trips." It looks like the number of afternoon trips is 569,438 v.s 395,533 morning trips. 

```sql
bq query --use_legacy_sql=false '
SELECT count(trip_id) AS Morning_Trips
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
where (Time(start_date) between "1:00:00" and "11:59:59") and (Time(end_date) between "1:00:00" and "11:59:59")'
```
Morning trips:

```
|Morning_Trips  |
+---------------+
|        395353 |
```

After trips:

```sql
bq query --use_legacy_sql=false '
SELECT count(trip_id) AS Afternoon_Trips
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
where (Time(start_date) between "12:00:00" and "23:59:59") and (Time(end_date) between "12:00:00" and "23:59:59")'
```

```
| Afternoon_Trips |
+-----------------+
|        569438   |
```

#### Project Questions

In this section I will list the main questions I need to answer to make recommendations. I have listed 9 questions and tried to answer all of them. Some of the questions might be revisited in the notebook with a different view. For each question, some recommendations are given if needed to increase the ridership.

- Q1: Find the top 5 bike stations with the highest number of unavailable bike events?
- Q2: Find the top 5 bike stations with the highest number of unavailable dock events?
- Q3: Number of trips by subscriber v.s casual customer?
- Q4: Find the most popular commuter trips?
- Q5: When is the peak demand in weekdays?
- Q6: When do the bikes_zero events peak?
- Q7: When do the docks_zero events peak?
- Q8: What are the top 10 stations with the highest bikes_availability being zero during peak demand hours?
- Q9: What are the top 10 stations with the highest bikes_availability during peak demand hours?


#### Answers

Q1: Find the top 5 most bike stations with the highest number of zero bike availability events?

Answering this question will help us spot those stations that raised “unavailable bike events” the most. It could be an indication for supply shortages in those stations probably because those are popular stations with high demand and not enough available bikes. One strategy to increase ridership could be to have more bikes available in those stations or invest in increasing the number of docks and bikes. In order to get the station name I joined the 'status' database with 'stations' on `station_id`. E. g. 2nd at Folsom is a station that generates the highest number of zero bike availability events. It means there are supply shortages there and increasing the number of bikes and docks might increase the ridership.  

```sql
SELECT 
      status.station_id,
      station.name, 
      count(status.station_id) AS zero_bike_event_number
FROM `bigquery-public-data.san_francisco.bikeshare_status` status
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` station ON status.station_id = station.station_id
where bikes_available = 0
group by station_id, station.name
order by zero_bike_event_number desc 
limit 5
```

```
Row	       station_id	     name	                         zero_bike_event_number
1	          62	         2nd at Folsom                     44844
2	          45	         Commercial at Montgomery          44728
3	          48	         Embarcadero at Vallejo            35903
4	          60	         Embarcadero at Sansome            32980
5	          41	         Clay at Battery                   32505

```

Q2: Find the top 5 bike stations with the highest number of unavailable docks events?

Answering this question will help us spot those stations that raised “unavailable dock events” the most. It could be an indication for most popular end stations where people normally just park the bikes so they run out of empty docks most often. Again there is an opportunity here to either increase the number of docks available in these stations or transfer bikes from these stations to the most popular stations for starting a trip or the ones which run out of bikes the most (identified in Q1). For example, San Francisco Caltrain is the station with the highest number of zero dock availability mostly because people which are traveling back from San Francisco to suburbs use Caltrain.

```sql
SELECT 
      status.station_id, 
      station.name, 
      count(status.station_id) AS zero_dock_event_number
FROM `bigquery-public-data.san_francisco.bikeshare_status` status
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` station ON status.station_id = station.station_id
where docks_available = 0
group by station_id, station.name
order by zero_dock_event_number desc 
limit 5
```

```
Row	 station_id	  name	                                           zero_dock_event_number
1	 70	          San Francisco Caltrain (Townsend at 4th)         43079
2	 54	          Embarcadero at Bryant                            39401
3	 73	          Grant Avenue at Columbus Avenue                  33112
4	 60	          Embarcadero at Sansome                           25476
5	 69	          San Francisco Caltrain 2 (330 Townsend)          23605
```

Q3: Number of trips by subscriber v.s casual customer?

Looks like the number of trips done by the subscribing members is significantly higher than the number of trips the casual customers have done. This seems to be a goal for Lyft to explore the incentives to turn the casual customers to the subscribing members to increase the ridership. 

```sql
SELECT subscriber_type, count(trip_id) as number_of_trips  
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
group by subscriber_type
```

```
Row	     subscriber_type	  number_of_trips	
1        Customer             136809
2	     Subscriber           846839
```

Q4: Find the most popular commuter trips?

This is an important question as it identifies trips that are performed on a regular basis mostly because of work. I defined the "commuter trip" as those trips from City A to City B. 

```sql
SELECT count(trip_id) as number_of_commuter_trips, 
    start_station_name, 
    station.landmark, 
    end_station_name, 
    station1.landmark 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` trip
join `bigquery-public-data.san_francisco.bikeshare_stations` station on trip.start_station_id = station.station_id
join `bigquery-public-data.san_francisco.bikeshare_stations` station1 on trip.end_station_id = station1.station_id
where station.landmark <> station1.landmark 
group by start_station_name,start_station_name, station.landmark, end_station_name, station1.landmark
order by number_of_commuter_trips desc
limit 5
```
```
Row number_of_commuter_trips start_station_name  landmark        end_station_name                   landmark_1
1   92          San Antonio Shopping Center      Mountain View   University and Emerson             Palo Alto
2   87          California Ave Caltrain Station  Palo Alto       San Antonio Caltrain Station       Mountain View
3   84          Stanford in Redwood City         Redwood City    Palo Alto Caltrain Station         Palo Alto
4   76          University and Emerson           Palo Alto       San Antonio Shopping Center        Mountain View
5   70          San Antonio Caltrain Station     Mountain View   California Ave Caltrain Station    Palo Alto

```

Q5: When is the peak demand in weekdays?

To answer this question, I extract the start hour of trips for those trips which are not in Saturday and Sunday and counted the trips. From the query result, it looks like the demand peaks in the morning between hours 8-9 and in the evening from 16-18. This cleary shows the demand peaks in the morning when people go to work and in the evening when they come back from work. 

```sql
SELECT start_hour, count(start_hour) start_hour_freq
from (
SELECT trip_id, EXTRACT(HOUR FROM start_date) AS start_hour, 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
WHERE (EXTRACT(DAYOFWEEK FROM start_date) NOT IN (1, 7)) and (EXTRACT(DAYOFWEEK FROM end_date) NOT IN (1, 7))
)
group by start_hour 
order by start_hour_freq desc
limit 5
```

```
Row      start_hour     start_hour_freq
1           8             128992
2           17            118298
3           9             90261
4           16            78966
5           18            78160
```

Q6: When do the zero bike availability events peak?

The following query put a filter on the weekdays and extract the start hours of the trips and group them by hours and count them. From the result of the query, it looks like hours 9-10 in the morning and 17-19 in the evening we see the highest number of bikes_available being zero. This is in line with the high demand hours which are the hours that people go to work or come back home. It looks like there is a lack of bikes in those hours the most. 

```sql
SELECT hour, count(hour) as zero_bike_hour_freq
from (
SELECT EXTRACT(HOUR FROM time) AS hour, 
FROM `bigquery-public-data.san_francisco.bikeshare_status` 
WHERE (EXTRACT(DAYOFWEEK FROM time) NOT IN (1, 7)) and bikes_available = 0
)
group by hour 
order by zero_bike_hour_freq desc
limit 10
```

```
Row      hour   hour_freq
1        18      93322
2        9       71380
3        17      66034
4        10      47665
5        19      47480
6        8       46415
```
Q7: When do the docks_zero events peak?

A similar query with a filter on the weekdays with docks_available being zero is created by extracting the start hours of the trips and grouping them by hours and counting them. From the result of the query, it looks like hours 9-10 in the morning and 18-19 in the evening we see the highest number of docks_available being zero. This is in line with the high demand hours which are the hours that people go to work or come back home. It looks like there is lack of docks to park bikes in those hours the most.

```sql
SELECT hour, count(hour) as zero_dock_hour_freq
from (
SELECT EXTRACT(HOUR FROM time) AS hour, 
FROM `bigquery-public-data.san_francisco.bikeshare_status` 
WHERE (EXTRACT(DAYOFWEEK FROM time) NOT IN (1, 7)) and docks_available = 0
)
group by hour 
order by zero_dock_hour_freq desc
limit 5
```

```
Row     hour   zero_dock_hour_freq	
1       9       30140	
2       10      26225	
3       18      23878	
4       23      19992	
5       19      19920	
```

Q8: What are the top 5 stations with the highest zero bikes_availability during peak demand hours?

A query is created that gives the top 10 station names, the frequency of bikes_available being zero and hours of thee events. The query is joining the status and station databases on station_id, putting a filter on stations that raise the zero bikes_available events the most during the weekdays. From the result, it looks like some stations in hours 9 am and 18 pm generate the zero bikes_available events the most. E. g. San Francisco Caltrain (Townsend at 4th) in hour 9 generated the highest number of events.

```sql
SELECT hour, name, count(hour) as zero_bike_hour_freq
from (
SELECT EXTRACT(HOUR FROM time) AS hour, station.name
FROM `bigquery-public-data.san_francisco.bikeshare_status` status
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` station on status.station_id = station.station_id
WHERE (EXTRACT(DAYOFWEEK FROM time) NOT IN (1, 7)) and bikes_available = 0
)
group by hour, name
order by zero_bike_hour_freq desc
limit 10
```

```
Row hour    name                                               zero_bike_hour_freq
1   9       San Francisco Caltrain (Townsend at 4th)               11128
2   18      Broadway St at Battery St                              10062
3   18      Embarcadero at Vallejo                                 7933
4   9       Temporary Transbay Terminal (Howard at Beale)          7677
5   18      Commercial at Montgomery                               7619
6   9       Grant Avenue at Columbus Avenue                        6771
7   9       Embarcadero at Bryant                                  6530
8   10      San Francisco Caltrain (Townsend at 4th)               6064
9   18      2nd at Folsom                                          6032
10  18      2nd at South Park                                      5903
```

Q9: What are the top 10 stations with the highest bikes_availability during peak demand hours?

In this query we want to explore if there are any staions with good amount of available bikes around those stations with zero bike availibility during peak hours. The following query shows the top ten stations with the most available bikes during peak hours. There is an opportunity here to increase ridership. For example, Japantown with the highest numbber of available bikes during peak hours is only 6.5 miles away from San Francisco Caltrain station which has the highest number of zero bike availability events. So they could either have a shuttle bus to take bikers from the San Francisco Caltrain station to the Japantown station or they could bring move available bikes from the Japantown station to the San Francisco Caltrain station and this will lower the  number of zero available bike events and increase the ridership and company's profit. Temporary Transbay Terminal is another station that creates high number of bike unavailability and it is about 2.3 miles away from Japantown.     

```sql
SELECT hour, name, count(hour) as bike_available_sum
from (
SELECT EXTRACT(HOUR FROM time) AS hour, station.name
FROM `bigquery-public-data.san_francisco.bikeshare_status` status
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` station on status.station_id = station.station_id
WHERE (EXTRACT(DAYOFWEEK FROM time) NOT IN (1, 7)) and (EXTRACT(HOUR FROM time) IN (9))
)
group by hour, name
order by bike_available_sum desc
limit 10
```

```
Row Hour  name                              bike_available_sum                            
1	9	  Japantown                           46339
2	9	  San Pedro Square                    46339
3	9	  MLK Library                         46339
4	9	  San Jose City Hall                  46339
5	9	  Paseo de San Antonio                46339
6	9	  Adobe on Almaden                    46339
7	9	  San Salvador at 1st                 46339
8	9	  Santa Clara at Almaden              46339
9	9	  San Jose Diridon Caltrain Station   46339
10	9	  San Jose Civic Center               46339
```

Similarly in the evening, the Japantown station has the highest number of bike availibility and is only 2.8 miles away from the Broadway St at Battery St station which is has the highest number of unavailable bike events during hour 18. Again a similar strategy to either transit some bikers with a shuttle bus or move some available bikes from the Japantown station to the Broadway St at Battery St station will definitely increase the ridership.

```sql
SELECT hour, name, count(hour) as bike_available_sum
from (
SELECT EXTRACT(HOUR FROM time) AS hour, station.name
FROM `bigquery-public-data.san_francisco.bikeshare_status` status
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` station on status.station_id = station.station_id
WHERE (EXTRACT(DAYOFWEEK FROM time) NOT IN (1, 7)) and (EXTRACT(HOUR FROM time) IN (18))
)
group by hour, name
order by bike_available_sum desc
limit 10
```

```
Row	   hour	    name                               bike_available_sum	
1	   18	    Japantown                             46601	
2	   18	    San Pedro Square                      46601	
3	   18	    SJSU 4th at San Carlos                46601	
4	   18	    MLK Library                           46601	
5	   18	    Paseo de San Antonio                  46601	
6	   18	    Adobe on Almaden                      46601	
7	   18	    San Jose City Hall                    46601	
8	   18	    Santa Clara at Almaden                46601	
9	   18	    San Jose Diridon Caltrain Station     46601	
10	   18	    San Jose Civic Center                 46601	
```
