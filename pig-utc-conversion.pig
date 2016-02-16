 //create a text file with comma seperated name and date of birth values in each row.
 data = LOAD 'hdfs:/burusoth/sample.txt' USING PigStorage(',') AS (name:chararray,birth_Date:chararray);
 loaded = FOREACH data GENERATE name, ToDate(ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss','UTC');
 DUMP loaded;