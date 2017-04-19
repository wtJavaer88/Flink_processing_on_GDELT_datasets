# bdc2017-flink-streaming

This repository includes a collection of examples for batch & stream processing of big data sets using [Apache Flink](https://flink.apache.org/). These examples have been used as part of the course [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/home) at the [Department of Computer, Control and Management Engineering](http://www.dis.uniroma1.it/en) of the [Sapienza University of Rome](http://en.uniroma1.it/).

The Apache Flink website provides a detailed [Setup and Quickstart guide](https://ci.apache.org/projects/flink/flink-docs-release-1.2/quickstart/setup_quickstart.html) as well as [examples](https://ci.apache.org/projects/flink/flink-docs-release-1.2/examples/) to get a better feel for Flink’s programming APIs. The examples related to the streaming engine of Flink are available at the [apache flink git hub repository](https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples). The Apache Flink website also includes a ver nice [guide for the streaming engine](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html).


## Data Set

The examples use the [City-wide Crime Statistics and Data Extracts](https://www.cityofsacramento.org/Police/Crime/Data-Extracts) provided by the Sacramento Police Departments of the City of Sacramento. All information is pulled from the Police Department Records Management System (RMS) and the Computer Aided Dispatch system (CAD). Because this data is produced via a complex set of processes, there are many places where errors can be introduced. The margin of error in this data is approximately 10%.

Visit the [City of Sacramento Open Crime Report Data Portal](http://data.cityofsacramento.org/home/) list to see Crime Data from two years ago, one year ago and the current year.

Download the [Sacramento Crime Data From Two Years Ago](http://data.cityofsacramento.org/dataviews/93309/sacramento-crime-data-from-two-years-ago/) as it is used for the examples. 

The dataset contains the following columns:
* Record ID - a unique identification for this crime report record.
* Offense - the crime classification coding and description. Check out the detailed list of the [SacPD Alpha Code List and State Crime Code](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/spdcodes.pdf?la=en) and the [Federal Uniform Offense Codes/Classifications](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/ucrcodes.pdf?la=en). For more information refere to the [Uniform Crime Reporting](https://ucr.fbi.gov/) website.
  * Offense Code
  * Offense Ext
  * Offense Category
  * Description
* Police District - The city is divided into 6 Districts. The Districts are divided into Beats, which are assigned to patrol officers. The Beats are divided into Grids for reporting purposes. See the [Map of Sacramento Neighborhoods and Police Beats](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/Maps/2015-Beat-Map-V2.pdf?la=en). 
  * Beat
  * Grid
* Occurence Date - The date and time of the crime report.
  * Occurence Time

Here is an extract from the dataset of 2015:

```
"Record ID","Offense Code","Offense Ext","Offense Category","Description","Police District","Beat","Grid","Occurence Date","Occurence Time"
"1073977","5213","5","WEAPON OFFENSE","246.3(A) PC NEGL DISCH FIREARM","2","2B","0541","01/01/2015","00:00:03"
"1074004","2404","0","STOLEN VEHICLE","10851(A)VC TAKE VEH W/O OWNER","1","1B","0401","01/01/2015","00:03:00"
"1074012","2404","0","STOLEN VEHICLE","10851(A)VC TAKE VEH W/O OWNER","6","6C","1113","01/01/2015","00:13:00"
```

## Batch Examples

We start by using some batch examples in order to understand the dataset and the basic concepts of Apache Flink. These examples are based on the [Apache Flink Use Case - Crima Data Analysis Part I](http://data-flair.training/blogs/apache-flink-use-case-crime-data-analysis/)
and [Apache Flink Use Case - Crima Data Analysis Part II](http://data-flair.training/blogs/apache-flink-real-world-use-case-crime-data-analysis-2/) available at [Data Flair](http://data-flair.training/blogs/).

### Analysis by District

We start by analyzing the input file based on the district where the crimes are reported.
For this example we use the [CrimeDistrict class](src/main/java/it/uniroma1/dis/bdc/batch/CrimeDistrict.java).

In this example we use the 6th column of the data set and count the number of crimes reported for each district.
We use a FlatMap to construct the two-valued tuples (district-id, 1) and in the sequel we request from Flink to
group the tuples based on the 1st value and sum the 2nd values.

The execution of this example is done as follows:

1. Make sure that the Apache Flink engine is up and running
```
(flink-1.2.0 installation directory)/bin/start-local.sh
Starting jobmanager daemon on host red.
```
2. Download the [2015 dataset](http://bit.ly/1WOB0Ih) and make it available under /tmp/crime.csv
3. Use maven to package the jar file
```
mvn package
```
4. Submit the batch job to flink
```
(flink-1.2.0 installation directory)/bin/flink run -c it.uniroma1.dis.bdc.batch.CrimeDistrict ./target/data-crime-0.1.jar --filename /tmp/crime.csv
```

The output produced should look like:
```
Cluster configuration: Standalone cluster with JobManager at localhost/127.0.0.1:6123
Using address localhost:6123 to connect to JobManager.
JobManager web interface address http://localhost:8081
Starting execution of program
Submitting job with JobID: 1647cf244fbb8bd48a7b578ae7d05eb0. Waiting for job completion.
Connected to JobManager at Actor[akka.tcp://flink@localhost:6123/user/jobmanager#300159966]
04/19/2017 12:10:14	Job execution switched to status RUNNING.
04/19/2017 12:10:14	CHAIN DataSource (at main(CrimeDistrict.java:53) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> FlatMap (FlatMap at main(CrimeDistrict.java:57)) -> Combine(SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to SCHEDULED
04/19/2017 12:10:14	CHAIN DataSource (at main(CrimeDistrict.java:53) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> FlatMap (FlatMap at main(CrimeDistrict.java:57)) -> Combine(SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to DEPLOYING
04/19/2017 12:10:14	CHAIN DataSource (at main(CrimeDistrict.java:53) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> FlatMap (FlatMap at main(CrimeDistrict.java:57)) -> Combine(SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to RUNNING
04/19/2017 12:10:14	Reduce (SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to SCHEDULED
04/19/2017 12:10:14	Reduce (SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to DEPLOYING
04/19/2017 12:10:14	CHAIN DataSource (at main(CrimeDistrict.java:53) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> FlatMap (FlatMap at main(CrimeDistrict.java:57)) -> Combine(SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to FINISHED
04/19/2017 12:10:14	Reduce (SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to RUNNING
04/19/2017 12:10:14	DataSink (collect())(1/1) switched to SCHEDULED
04/19/2017 12:10:14	DataSink (collect())(1/1) switched to DEPLOYING
04/19/2017 12:10:14	DataSink (collect())(1/1) switched to RUNNING
04/19/2017 12:10:14	Reduce (SUM(1), at main(CrimeDistrict.java:59)(1/1) switched to FINISHED
04/19/2017 12:10:14	DataSink (collect())(1/1) switched to FINISHED
04/19/2017 12:10:14	Job execution switched to status FINISHED.
(,9)
(1,5607)
(2,7754)
(3,6926)
(4,5463)
(5,5259)
(6,8757)
(UI,445)
Program execution finished
Job with JobID 1647cf244fbb8bd48a7b578ae7d05eb0 has finished.
Job Runtime: 953 ms
Accumulator Results:
- d35de0db30bfd74cb9316104645b7f32 (java.util.ArrayList) [8 elements]
```

Note that in the above execution there are 9 rows with a missing district ID and 445 records with a wrong district ID.


