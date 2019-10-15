# Building Energy Consumption

## Introduction

This is a data engineering project based on the Google Cloud Platform, specifically utilizing the Pub/Sub-Dataflow-BigQuery severless stream processing. The raw data was sourced from [Schneider Electric Exchange](https://shop.exchange.se.com/home), under ["Buildings energy consumption measurements"](https://shop.exchange.se.com/apps/39117/buildings-energy-consumption-measurements#!overview) There are two problems that this project aims to solve:

1. Batch load each building's energy consumption data for data analysts or data scientists to perform analysis.
1. Calculate the average energy consumption of each building in real time and provide the data in real time for monitoring.

## Table of Contents

1. [Architecture](#architecture)
1. [Data Simulation](#data-simulation)
1. [Data Pipeline](#data-pipeline)
1. [BigQuery Schema](#)
1. [Results](#results)
1. [How To Run on GCP](#how-to-run-on-gcp)
1. [Further Improvements](#further-improvements)
1. [Licensing](#licensing)

## Architecture

<p align="center"><img src="/imgs/architecture.png"></img></p>

- Ingestion Layer (Compute Engine, Cloud Pub/Sub):

  - In a realistic scenario, the sensor to Pub/Sub architecture would look something like this:
       <p align="center"><font size="-1">
    <img src="https://storage.googleapis.com/gcp-community/tutorials/cloud-iot-gateways-rpi/gateway-arch.png" width="70%"></br>
          <i>Source: <a href="https://cloud.google.com/community/tutorials/cloud-iot-gateways-rpi)*">"Using Cloud IoT Core gateways with a Raspberry Pi"</a></i></font></p>
  - Since the original data is a historical data of the energy consumption, a compute engine instance was used to simulate the ingested data published to Cloud Pub/Sub architecture.

- Batch Layer (Cloud Dataflow, BigQuery): With the Apache Beam's PubsubIO, the messages published from the ingestion layer was read, translated into BigQuery rows, and were loaded to BigQuery.

- Stream Layer (Cloud Dataflow, BigQuery, Cloud Pub/Sub): On top of ingesting the data using PusubIO with the batch layer, the real time analysis of running average of the main meter readings of each building was conducted. The results were both stored in BigQuery and also published to a separate topic on Cloud Pub/Sub in case of creating a web interface for serving the real time data publicly.

## Data Simulation

### Structure of the Original Data

As seen on the [original csv](./data/buildings-energy-consumption-clean-data.csv) exported from [Schneider Electric Exchange](https://shop.exchange.se.com/home), the original schema of the csv was:

| Timestamp | 1_Main Meter_Active energy | 1_Sub Meter Id1_Active energy | ... | 8_Sub Meter Id9_Active energy |
| --------- | -------------------------- | ----------------------------- | --- | ----------------------------- |
| 2017-04-02T02:15:00-04:00 | 17779.0 | 3515.0 | ... | 361.0 |

The timstamp used the UTC ISO format, and the energy readings were taken every fifteen minutes, read in Watt-hour.

### Restructured Raw Data for Simulation

Since the original data was cleaned manually and all of the building data were gathered into a single table  by whoever posted the data on the online library, I wanted to adjust the schema to something more realistic in a scenario where multiple sensors from multiple buildings were sending their data. I assumed that the IoT Gateway that I tried to simulate received the data on building to building basis, and performed the most minimal function possible to reach Cloud Pub/Sub (e.g. gathering multiple sensor data with common building location to be aggregated into a single row). Example schema of building 1 and building 8 are shown below:

| timestamp | building_id | Gen | Sub_1 | Sub_3 |
| --------- | ----------- | --- | ----- | --- |
| YYYY-MM-DD HH:MM:SS | 1 | 17779.0 | 3515.0 | 1942.0 |

| timestamp | building_id | Gen | Sub_1 | Sub_10 | Sub_11 | Sub_9 |
| --------- | ----------- | --- | ----- | ------ | ------ | ----- |
| YYYY-MM-DD HH:MM:SS | 8 | 16039.0 | 4471.0 | 253.0 | 2938.0 | 361.0 |

### Pub/Sub, SpeedFactor, and Event Timestamps

When running the `send_meter_data.py` to lauch the data publishing simulation to Pub/Sub, the user must provide the `speedFactor`. The `speedFactor` allows the user to quicken the simulation of data. For example, if the user provides the SpeedFactor of 60, one event row of the original data will be sent per minute. More accurately, after the change in the schema, 8 rows will be published per minute (although the order of arrival of the messages won't be the same every time due to latency). To match the original data time increment, the user must provide the SpeedFactor of 900, meaning one event per 15 minutes.

Along with splitting the original rows of data in `send_meter_data.py` as explained in [Restructured Raw Data for Simulation](#restructed-raw-data-for-simulation), event timestamps were altered to match real time prior to publishing on Pub/Sub to reflect the appropriate real time behavior.

## Data Pipeline

![Dataflow DAG](./imgs/dataflow.png)

The Dataflow DAG above provides the step by step view of how the data was ingested, aggregated, and loaded, or stream inserted. Starting from the top, the data was read and ingested from Cloud Pub/Sub. Then, the pipeline was branched to the stream (on the left), and batch (on the right) processing. In the stream processing, the `SlidingWindows` was set, and the general meter readings of each building was aggregated according to the window created to calculate the Mean. The `SlidingWindows` took two requird arguments -- `size` and `period`. The size indicates how wide the window should be in seconds, and the period indicates for how long (in seconds) the aggregation must be recalculated. A sliding window of 60 seconds with period of 30 seconds would look something like this:

 <p align="center"><font size="-1">
    <img src="https://beam.apache.org/images/sliding-time-windows.png" width="70%"></br>
          <i>Source: <a href="https://beam.apache.org/documentation/programming-guide/)*">"Beam Programming Guide"</i></a></font></p>

## Results

![pubsub_in](./imgs/pubsubInput.png)

![bq_history](./imgs/bq_historyData.png)

![bq_avgs](./imgs/bq_avgData.png)

![pubsub_out](./imgs/pubsubAvgMessages.png)

## How To Run on GCP
Here are the simple steps to running this project on your own GCP console.
1. Create a new VM instance on Compute Engine.
1. Open two windows of the VM instance by clicking the 'SSH' button
1. create and export these environment variables:
   * PROJECT_ID=[your project id]
   * BUCKET=[your GCP storage bucket]
1. Setup git and python (Python2, specifically for this version of the project)
1. 

## Further Improvements
### Improvements for Optimization
- Partition the tables by time, to improve query performance and control costs read by a query. The Partition would be Date/time based, rather than the ingestion time so that the data analysts or scientists could retrieve the data that they desire from a specific time frame of the events. However, at what point of time the partition is made must be carefully chosen due to the Google's quota of 4,000 maxmimum number of partitions per partitioned table. Further details on the BigQuery quota can be found [here](https://cloud.google.com/bigquery/quotas) and pricing can be found [here](https://cloud.google.com/bigquery/pricing)

- Split the pCollection at the beginning based on the `building_id` rather than the two separate times during stream and batch processings. Currently, this operation is repetitive since the stream processing's `GroupByKey` operation groups the key value pairs based on the key of `building_id` and the batch processing contains 8 separate filter functions to filter out the corresponding `building_id` data to load to its table. Although the current size of data is small enough to overlook this aspect, once the data becomes wider, this will be a costly process.

- Figure out a way to unify the submeter labels. The raw data had submeter ids that were almost random and varying in size (building 1 had submeter 1 and 3 while building 7 had submeter 1, 10, 11, and 9), which caused the BigQuery load tables to have 8 separate tables, one for each building. Instead of this, by establishing a clear protocol of expressing the submeters, the data can be combined into a single table, and have the energy values be NULLABLE. 

- For future cases of having the data be scaled up (more submeters, more buildings, other parameters like temperature, humidity, etc.), Cloud BigTable (HBase is based on the concept of BigTable) would be a choice to consider. Cloud Bigtable is specifically designed for sparsely populated tables to handle billions of rows and thousands of columns, and its popular use case is timeseries data. Although Cloud Bigtable is a noSQL database, the data analysts or scientists who are more familiar with the SQL syntax can use BigQuery to query data stored in Cloud Bigtable. It supports high read and write throughput at low latency, so it would not be a problem to store the running average data, which requires frequent udpates. 

### Miscellaneous Improvements
- Write the code in Python3, since Python2 is deprecated for Apache Beam (will stop serving for 2.7 starting on January 1st, 2020). Although the code was originally written in Python3, the VM instance on GCP used Python2 by default, so for saving time on setting up the environment, the code was edited to match Python2.

- Create a serving layer for the Pub/Sub output message can be viewed through a UI.

## Licensing

This project uses the [Apache License 2.0](LICENSE)
