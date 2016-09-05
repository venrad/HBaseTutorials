# HBaseTutorials
This project is to capture all the Hbase related scenarios. 

1. Hbase BulkLoader using MapReduce 

Bulk Loading is the MapReduce job that would read the source data (Type of input is handled in the mapper). Mapper should spit the output as `<ImmutableByteWritable>, <Put>` or `<ImmutableBytesWritable>, <KeyValue>`
