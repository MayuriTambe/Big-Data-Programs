REGISTER '/home/hadoopuser/Desktop/Pig/udfs/DefinedFunctions.py' USING streaming_python AS Functions; 

File = LOAD '/home/hadoopuser/Desktop/Pig/udfs/user_info.csv' USING PigStorage(',') AS (username:chararray,idle_time:chararray,working_hour:chararray,start_time:chararray,end_time:chararray);

REGISTER '/home/hadoopuser/Desktop/pig-0.17.0/contrib/piggybank/java/piggybank.jar';

avg_workinghours= FOREACH File GENERATE Functions.get_user_working_hours(working_hours);

higher_Workinghours=CROSS  Functions.get_user_working_hours ,  avg_workinghours;

users_highest_workinghours=foreach higher_Workinghours GENERATE Functions.get_user_working_hours>avg_workinghours;

DUMP users_highest_workinghours;
