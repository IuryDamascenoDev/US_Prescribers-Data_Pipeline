# Pipeline for ETL US Prescribers Data, creatung multiple reports based on requirements

## Pipeline Flowchart
![Pipeline Flowchart](pipeline_flowchart.png "Pipeline Flowchart")

## Steps
### Data ingestion
1. Data upload to GCS Bucket for availability.  

### Data preprocessing
1. Performed data cleaning for Cities and Prescribers datasets.  
2. Performed transformations to make data suitable for joining.  

### Data transform
1. Creation of reports based on requirements.  
&emsp; 1.1 Prescribers report requirements:  
&emsp;&emsp;&nbsp;&nbsp; Top 5 Prescribers with highest trx_cnt per each state.  
&emsp;&emsp;&nbsp;&nbsp; Consider the prescribers only from 20 to 50 years of experience.  
&emsp;&emsp;&nbsp;&nbsp; Table Model:  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber ID  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber Full Name  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber State  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber Country  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber Years of Experience  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Total TRX Count  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Total Days Supply  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Total Drug Cost  

&emsp;&emsp; 1.2 Cities report requirements:  
&emsp;&emsp;&nbsp;&nbsp; Calculate the Number of zips in each city.  
&emsp;&emsp;&nbsp;&nbsp; Calculate the number of distinct Prescribers assigned for each City.  
&emsp;&emsp;&nbsp;&nbsp; Calculate total TRX_CNT prescribed for each city.  
&emsp;&emsp;&nbsp;&nbsp; Do not report a city in the final report if no prescriber is assigned to it.  
&emsp;&emsp;&nbsp;&nbsp; Table Model:  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; City Name  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; State Name  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; County Name  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; City Population  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Number of Zips  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Prescriber Counts  
&emsp;&emsp;&emsp;&emsp;&nbsp;&nbsp; Total Trx counts  

### Data load
1. Load transformed data to HDFS for optimization for further reading and/or transformations.  
2. Load transformed data to GCS for avalability and presentation.  

## This pipeline includes the usage of following tools and languages  
1. Python and PySpark  
2. Google Cloud Platform, Cloud Storage, Dataproc and GCP CLI  
3. Hadoop File System HDFS  
4. Logging for optimizing debug stages and testing  
