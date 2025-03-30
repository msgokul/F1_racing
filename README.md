# Azure End-to-End Project on Formula1 Racing dataset 

## <u>Introduction</u>
This project demosntrates an end-to-end data engineering pipeline solution to derive useful information from raw Formula1 Racing dataset downloaded from Ergast API utlizing key Azure data engineering cloud services like Azure Data Lake Storage Gen2, Azure Key Vault, Azure Data Factory, and Azure Databricks. The useful information derived from the end stage of the project has been utilised to derive key insights by creating reports in Power BI. The project is build upon the Medallion Architecture. This project was developed as part of a <a href ="https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?couponCode=25BBPMXACCAGE2"> Udemy course </a> by Ramesh Retnasamy.

## <u>Medallion Architecture</u>

![Medallion Architecture](https://github.com/user-attachments/assets/fb66abbf-6988-4788-9ca0-67ecfb9a6597)


The project is build upon Medallion Architecture where the data in different form passes through 3 stages to obtain the final output. The 3 stages are:
<ol type="1">
  <li>
  <b>Bronze Layer (Raw Data)</b>: Raw datasets in the csv and json format downloaded from Ergast API were ingested and stored .
  </li>
  <li>
  <b>Silver Layer (Processed Data)</b>: The raw datasets from the Bronze Layer were invoked in this layer and simple data pre-processing techniques like handing null entries, standardizing formats, type formatting and so on were applied. The processed datasets were stored in <b>Parquet format</b>.
  </li>
  <li>
  <b>Gold Layer (Transformed Data)</b>: The processed datasets from the Silver Layer were invoked in this layer and transformation techinques like aggregation, joining, model structuring were applied in order to produce meaningful reports. The transformed data were stored in <b>Delta Lake</b> which supports ACID transactions, time travel and stores audit logs.
  </li>
</ol>

## <u>Datasets</u>

The image below depicts the Entity Relationship Duiagram of the datasets. The data model follows a structured approach based on an Entity Relationship Diagram (ERD) to ensure seamless data integration and analysis.

![formula1_ergast_db_data_model](https://github.com/user-attachments/assets/f2e8a511-0ed1-43e3-a5aa-c891e0c0ac80)

#### Key Entities :
<ul>
  <li>
    <b>Races:</b> Contains details about each race, including location, date, and circuit information.
  </li>
  <li>
    <b>Drivers:</b> Stores driver information such as name, nationality, and team association.
  </li>
  <li>
    <b>Constructors:</b> Represents F1 teams with relevant details like sponsors and constructors.
  </li>
   <li>
    <b>Lap Times:</b> Captures lap-by-lap information including lap time, and position.
  </li>
  <li>
    <b>Pit Stops:</b> Records pit stop data like dutration, stop, and time.
  </li>
  <li>
    <b>Qualifying:</b> Inculdes qualifying results information of a specific race like number, position, driver ID, and Constructor ID. 
  </li>
  <li>
    <b>Circuits:</b> Comprises of the circuit data such as name, location and so on
  </li>
  <li>
    <b>Results:</b> Includes data regarding the results of a specific race like driver details, constructor details, positions and points
  </li>
</ul>

## <u>Tools & Technologies Used</u>

The project integrates various Azure data engineering and analytical tools:

<ul>
  <li>
    <b>Azure Data Lake Storage Gen2:</b> Stores data passing through different stages in containers. 
  </li>
  <li>
    <b>Azure Key Vault & Azure Active Directory:</b> Facilitates authentication, secrets management and protection of sensitive data.
  </li>
  <li>
    <b>Azure Databricks:</b> To author workbooks for implementing various techniques like data ingestion, data processing and data tranformation utilizing PySpark and Spark SQL.
  </li>
  <li>
    <b>Azure Data Factory:</b> Manages ETL workflows for data ingestion, data processing and data transformation
  </li>
  <li>
    <b>Power BI:</b> Creates interactive reports and visual analytics
  </li>
</ul>







