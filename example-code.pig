--Register required libraries
REGISTER /usr/lib/hbase/lib/*.jar;

--declare variables that are going to be used as params. Sjould be inside ''
%DECLARE variable_name 'sample_input';

--load from hive ware house. Specify schema. There should be a space between = , LOAD
input_data = LOAD '$hive_db_name.$hive_table' USING org.apache.hcatalog.pig.HCatLoader() AS(a:chararray,b:chararray);

--Filter null values
input_filttered = FILTER input_data BY a IS NOT NULL AND NOT(a MATCHES 'NULL');

--use forEach to alter rows
input_name_list = FOREACH input_filttered GENERATE a;

--use distinct key word to remove duplicate words
distinct_name_list = DISTINCT input_name_list;

--load hbase data into pig
hbase_data = LOAD 'hbase://$hbase_table_name'
               USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rec:user_id','-loadKey true') AS (id:chararray:value:bag{(b:chararray,c:chararray)});

--join based on column
joined_data = JOIN data1 BY id, data2 BY index;

--union two tables
unioned_data = UNION data1, data2;

--group by single column, more than 1 column
grouped_data = GROUP unioned_data BY (a,b)

--nested ForEach
removed_data = FOREACH grouped_data {
	FOREACH grouped_data GENERATE b, c;
}

--check bulit in functions for date time after pig version 0.12
--ToDate
--DaysBetween

--Add records to HBase
STORE removed_data INTO 'hbase://$table_name' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('rec:id rec:a');



