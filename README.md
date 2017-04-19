# bdc2017-flynk-streaming

This repository includes a collection of examples for batch & stream processing using Apache Flynk. These examples have been used as part of the course [Big data computing](https://piazza.com/uniroma1.it/spring2017/1044406/home) at the [Department of Computer, Control and Management Engineering](http://www.dis.uniroma1.it/en) of the [Sapienza University of Rome](http://en.uniroma1.it/).

## Data Set

The examples use the [City-wide Crime Statistics and Data Extracts](https://www.cityofsacramento.org/Police/Crime/Data-Extracts) provided by the Sacramento Police Departments of the City of Sacramento. All information is pulled from the Police Department Records Management System (RMS) and the Computer Aided Dispatch system (CAD). Because this data is produced via a complex set of processes, there are many places where errors can be introduced. The margin of error in this data is approximately 10%.

Visit the [City of Sacramento Open Crime Report Data Portal](http://data.cityofsacramento.org/home/) list to see Crime Data from two years ago, one year ago and the current year.

Download the [Sacramento Crime Data From Two Years Ago](http://data.cityofsacramento.org/dataviews/93309/sacramento-crime-data-from-two-years-ago/) as it is used for the examples. 

The dataset contains the following columns:
* Record ID - a unique identification for this crime report record.
* Offense - the crime classification coding and description. Check out the detailed list of the [SacPD Alpha Code List and State Crime Code](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/spdcodes.pdf?la=en) and the [Federal Uniform Offense Codes/Classifications](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/ucrcodes.pdf?la=en). For more information refere to the [Uniform Crime Reporting](https://ucr.fbi.gov/) website.
..* Offense Code
..* Offense Ext
..* Offense Category
..* Description
* Police District - The city is divided into 6 Districts. The Districts are divided into Beats, which are assigned to patrol officers. The Beats are divided into Grids for reporting purposes. See the [Map of Sacramento Neighborhoods and Police Beats](https://www.cityofsacramento.org/-/media/Corporate/Files/Police/Crime/Maps/2015-Beat-Map-V2.pdf?la=en). 
..* Beat
..* Grid
* Occurence Date - The date and time of the crime report.
..* Occurence Time

