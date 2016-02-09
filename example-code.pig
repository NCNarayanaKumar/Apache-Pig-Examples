--Register required libraries
--REGISTER /usr/lib/hbase/lib/*.jar;

--declare variables that are going to be used as params. Sjould be inside ''
%DECLARE hive_db_name 'hive_database';
%DECLARE hive_table 'hive_table';

--load from hive ware house. Specify schema. There should be a space between = and LOAD
hive_data = LOAD '$hive_db_name.$hive_table' USING org.apache.hcatalog.pig.HCatLoader() AS(id:chararray,name:chararray,date-of-birth:chararray);

--Filter records if id consist null values
input_filttered = FILTER hive_data BY id IS NOT NULL AND NOT(id MATCHES 'NULL');

--use ForEach Generate to extract columns
input_name_list = FOREACH input_filttered GENERATE id;

--use distinct key word to remove duplicate words
distinct_name_list = DISTINCT input_name_list;

--load hbase data which is in another format
hbase_data = LOAD 'hbase://$hbase_table_name'
               USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rec:user_id','-loadKey true') AS (id:chararray:value:bag{(name:chararray,date-of-birth:chararray)});

--join based on column
joined_data = JOIN distinct_name_list BY id, data2 BY hbase_data;

--flatten hbase data
flatten_data = FOREACH hbase_data {
	GENERATE $0 AS id, FLATTEN($1) AS (name:chararray,date-of-birth:chararray);
};

--union two tables
unioned_data = UNION hive_data, flatten_data;

--group by single column, more than 1 column
grouped_data = GROUP unioned_data BY (id,name);

--remove unwanted data after grouped
--nested ForEach
removed_data = FOREACH grouped_data {
	FOREACH unioned_data GENERATE FLATTEN(group),date-of-birth;
};

--check bulit in functions for date time after pig version 0.12
--ToDate
--DaysBetween

--Add records to HBase
STORE removed_data INTO 'hbase://$table_name' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rec:id rec:values');



