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


