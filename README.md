# Azure End-to-End Project on Formula1 Racing dataset 

### Introduction
This project demosntrates an end-to-end data engineering pipeline solution to derive useful information from raw Formula1 Racing dataset downloaded from Ergast API utlizing key Azure data engineering cloud services like Azure Data Lake Storage Gen2, Azure Key Vault, Azure Data Factory, and Azure Databricks. The useful information derived from the end stage of the project has been utilised to derive key insights by creating reports in Power BI. The project is build upon the Medallion Architecture. This project was developed as part of a <a href ="https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?couponCode=25BBPMXACCAGE2"> Udemy course </a> by Ramesh Retnasamy.

### Medallion Architecture

![Medallion Architecture](https://github.com/user-attachments/assets/0b22dc24-1dcc-422c-9f94-dd87ccb51df3)

The project is build upon Medallion Architecture where the data in different form passes through 3 stages to obtain the final output. The 3 stages are:
<ol type="1">
  <li>
  <b>Bronze Layer (Raw Data)</b>: Raw datasets in the csv and json format downloaded from Ergast API were ingested and stored .
  </li>
  <li>
  <b>Silver Layer (Processed Data)</b>: The raw datasets from the Bronze Layer were invoked in this layer and simple data pre-processing techniques like handing null entries, standardizing formats, type formatting and so on were applied.
  </li>
  <li>
  <b>Gold Layer (Transformed Data)</b>: The processed datasets from the Silver Layer were invoked in this applied and transformation techinques like aggregation, joining, model structuring were applied in order to produce reports.
  </li>
</ol>


