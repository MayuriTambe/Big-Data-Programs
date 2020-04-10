REGISTER '/home/hadoopuser/Desktop/Pig/udfs/DefinedFunctions.py' USING streaming_python AS Functions; 

REGISTER '/home/hadoopuser/Desktop/pig-0.17.0/contrib/piggybank/java/piggybank.jar';

File = LOAD '/home/hadoopuser/Desktop/Pig/udfs/user_info.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,end_time:chararray);


usersname= FOREACH data GENERATE Functions.get_user_name(user_name), Functions.get_end_time(end_time);

user_orders= ORDER usersname BY end_time ASC;

user_late_commers= LIMIT user_orders 20;

DUMP user_late_commers;


