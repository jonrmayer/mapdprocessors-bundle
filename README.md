# mapdprocessors-bundle

This Nifi Processor Project targets Nifi version 1.5 and MapD 4.1.0

It current has a dependency on this maven project - https://github.com/jonrmayer/mapd_thrift_maven

and https://github.com/jonrmayer/mapdservices-bundle

There is currently one processor - PutMapDProcessor which ingests data into a pre created table within MapD

The Test uses the following table:- CREATE TABLE  TEST(TEST_STRING TEXT ENCODING DICT,TEST_FLOAT FLOAT,TEST_DATE DATE,TEST_DECIMAL DECIMAL(10),TEST_TIMESTAMP TIMESTAMP)


There are a number of future processors that can be built such as:-
1)MapD Create Table Processor based upon inferring the avro schema
2) MapD GetMapDProcessor - get data out of MapD
3) Spatial Data ingest processor


The code is a "mash-up" between Apache licensed projects such as
 mapd-core - specifically SQLImporter.java 
and 
Streamsets data collector api - specifically https://github.com/streamsets/datacollector-api/tree/master/src/main/java/com/streamsets/pipeline/api/impl for data conversion between avro back to java types








