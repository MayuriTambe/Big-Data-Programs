REGISTER '/home/hadoopuser/Desktop/Pig/udfs/DefinedFunctions.py' USING streaming_python AS Functions;

REGISTER '/home/hadoopuser/Desktop/pig-0.17.0/contrib/piggybank/java/piggybank.jar';

File = LOAD '/home/hadoopuser/Desktop/Pig/udfs/user_info.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,end_time:chararray);

usersname= FOREACH data GENERATE Functions.get_user_name(user_name), Functions.get_idle_time(idle_time);

highest_user_hours= ORDER usersname BY idle_time DESC;

user_limits= LIMIT highest_user_hours 20;

DUMP user_limits;
