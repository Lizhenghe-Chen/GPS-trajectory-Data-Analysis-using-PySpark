# GPS-trajectory-Data-Analysis-using-PySpark
In this assignment, we are going to use PySpark to analyse the GPS trajectory dataset that was collected in the Geolife project (Microsoft Research Asia) over a period of five years by 100+ people. Each GPS trajectory in this dataset is represented by a sequence of time-stamped points, each of which contains the information of latitude, longitude and altitude.
Spark is a very useful and convenient tool to deal with super large datasets, which much faster than Pandas dataframe. 

A more detailed description of the dataset and how it was collected can be found here:

https://www.microsoft.com/en-us/research/project/geolife-building-social-networks-using-human-location-history/

The first line of the file contains a header:

UserID,Latitude,Longitude,AllZero,Altitude,Timestamp,Date,Time

which is self-explanatory (you can ignore the 0s in the AllZero column). The Timestamp is the number of days (with fractional part) that have passed since 12/30/1899. You should process this data as a RDD or Spark’s DataFrame.

![image](https://user-images.githubusercontent.com/61171413/151579156-15e88294-3102-440c-a5db-54765dc2d574.png)


To simplify matters you can interpret (longitude, latitude) as a (x,y) point in 2D space and calculate the distance between two such points using the standard Euclidean distance. However, to be accurate, you should use one of the solutions presented here to calculate this distance (pick any that works for you and make sure it calculates the distance correctly based on some test example):

https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude (Links to an external site.)

1. Any data analysis starts with data cleaning (in fact this typically takes most of the time). In this case, we should convert all dates/times from GMT to Beijing time, where essentially all these trajectories were collected. This requires to move dates, times and timestamps by 8 hours ahead. You should not create a new input file, but instead use Spark’s map/withColumn transformation to change the RDD/DataFrame created from the original file. [20 pt] (If you find this too difficult, just skip to the next point and say so in your report. You will just simply miss out on the points for this task as a result.)
2. Calculate for each person, on how many days was the data recorded for them (count any day with at least one data point). Output the top 5 user IDs according to this measure and its value (as mentioned above, in case of a tie, output the user with the smaller ID). [20 pt]
3. Calculate for each person, on how many days there were more than 100 data points recorded for them (count any day with at least 100 data points). Output all user IDs and the corresponding value of this measure. [20 pt]
4. Calculate for each person, the highest altitude that they reached. Output the top 5 user ID according to this measure, its value and the day that was achieved (in case of a tie, output the earliest such a day). [20 pt]
5. Calculate for each person, the timespan of the observation, i.e., the difference between the highest timestamp of his/her observation and the lowest one. Output the top 5 user ID according to this measure and its value. [20 pt]
6. Calculate for each person, the distance travelled by them each day. For each user output the (earliest) day they travelled the most. Also, output the total distance travelled by all users on all days. HINT: use lag and window functions. [20 pt]
