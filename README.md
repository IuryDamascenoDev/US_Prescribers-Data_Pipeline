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

&emsp; &emsp; 1.1 Prescribers report requirements:  
&emsp; &emsp; &emsp; &emsp; Top 5 Prescribers with highest trx_cnt per each state.  
&emsp; &emsp; &emsp; &emsp; Consider the prescribers only from 20 to 50 years of experience.  
&emsp; &emsp; &emsp; &emsp; Table Model:  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber ID  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber Full Name  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber State  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber Country  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber Years of Experience  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Total TRX Count  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Total Days Supply  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Total Drug Cost  

&emsp; &emsp; 1.2 Cities report requirements:  
&emsp; &emsp; &emsp; &emsp; Calculate the Number of zips in each city.  
&emsp; &emsp; &emsp; &emsp; Calculate the number of distinct Prescribers assigned for each City.  
&emsp; &emsp; &emsp; &emsp; Calculate total TRX_CNT prescribed for each city.  
&emsp; &emsp; &emsp; &emsp; Do not report a city in the final report if no prescriber is assigned to it.  
&emsp; &emsp; &emsp; &emsp; Table Model:  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; City Name  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; State Name  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; County Name  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; City Population  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Number of Zips  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Prescriber Counts  
&emsp; &emsp; &emsp; &emsp; &emsp; &emsp; Total Trx counts  

### Data load
1. Load transformed data to HDFS for optimization for further reading and/or transformations.  
2. Load transformed data to GCS for avalability and presentation.  

## This pipeline includes the usage of following tools and languages  
1. Python and PySpark  
2. Google Cloud Platform, Cloud Storage, Dataproc and GCP CLI  
3. Hadoop File System HDFS  
4. Logging for optimizing debug stages and testing  
