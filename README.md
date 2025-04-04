# Azure End-to-End Project on Formula1 Racing dataset 

## <u>Introduction</u>
This project demonstrates an end-to-end data engineering pipeline solution to derive useful information from raw Formula1 Racing dataset downloaded from Ergast API utilizing key Azure data engineering cloud services like Azure Data Lake Storage Gen2, Azure Key Vault, Azure Data Factory, and Azure Databricks. The useful information derived from the end stage of the project has been utilized to derive key insights by creating reports in Power BI. The project is build upon the Medallion Architecture. This project was developed as part of a <a href ="https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?couponCode=25BBPMXACCAGE2"> Udemy course </a> by Ramesh Retnasamy.

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

The image below depicts the Entity Relationship Diagram of the datasets. The data model follows a structured approach based on an Entity Relationship Diagram (ERD) to ensure seamless data integration and analysis.

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

## <u>Project Methodology</u>

#### <u>1. Getting Started </u>

<ul>
  <li>
    Create Free account on <a href="https://portal.azure.com">Azure Portal.
  </li>
  <li>
    Create a Resource Group that is specifc for the project.
  </li>
  <li>
    Create resources within the Rescource Group which includes Azure Databricks, Azure Data Lake Storage (ADLS)and Azure Data Factory(ADF).
  </li>
  <li>
    Create 3 containers in the ADLS namely <i>raw</i>, <i>processed</i>, <i>presentation</i>
  </li>
  <li>
    Setup the workspace and clusters as required depending on the project in Azure Databricks
  </li>
  <li>
    Configure ADF for creating workflows as needed for the project.
  </li>
  <li>
    Create and setup Azure KeyVault and Azure Active Directory for secrets management and authentication
  </li>
  <li>
    Establishing connection between Power BI and Delta Lake tables
  </li>
</ul>

#### <u>2. Data Ingestion</u>

Raw datasets downloaded from Ergast API have been directly uploaded in to the <b>Raw container</b> created in Azure Data Lake Storage. The datasets were organized in to 3 folders for 3 different dates to faciliate incremental load which will based on these dates. Multiple datasets in csv and json format were available. 

#### <u>3. Data Processing</u>

<ul>
  <li>
    The datsets from Raw container were imported in Azure Databricks as <i>Dataframes</i> on which data pre-processing was applied utilizing PySpark. 
  </li>
  <li>
    Standard data pre-processing techniques like handling null values, type formatting, standardizing formats were applied.
  </li>
  <li>
    The processed dataframes were saved as tables in Delta Lakes in the <b>Silver layer</b>, which is the <i>processed</i> container in the Azure Data Lake Storage. 
  </li>
  <li>
    Delta Lakes are optimsed storage layer that supports ACID transaction. Delta Lakes facilitates CRUD operations, reliability, time travel and faster querying.
  </li>
</ul>

#### <u>4. Data Transformation
 <ul>
   <li>
     The processed data from Silver Layer were imported in Azure Databricks as <i>Dataframes</i> on which tranformation techniques were applied utilizing PySpark and Spark SQL
   </li>
    <li>
      Tranformation techniques like aggreagations, joining, and model structuring were applied.
    </li>
   <li>
     Utilizing views in Spark SQL 3 <b>Dimensional Tables</b> and 1 <b>Fact table</b> were created and were stored in Delta Lake.
   </li>
 </ul>

#### <u>5. ETL workflows</u>

<ul>
  <li>
    ETL workflow comprises of 2 pipelines: <b>Transform</b> and <b>Analyze</b>. The pipelines were created and published in Azure Data Factory.
  </li>
  <li>
    <b>Transform Pipeline: </b>In this pipeline, data stored in JSON and CSV format is read using Apache Spark with minimal transformation saved into a delta table. The transformation includes dropping columns, renaming headers, applying schema, and adding audited columns (ingestion_date and file_source) and file_date as the notebook parameter. This serves as a dynamic expression in ADF.
  </li>
  <li>
    <b>Analyze Pipeline: </b>In the second pipeline, Databricks SQL reads preprocessed delta files and transforms them into the final dimensional model tables in delta format after perofrming analyzing techniques. Transformations performed include dropping duplicates, joining tables using join, and aggregating using window functions.
  </li>
</ul>

A<b> Master pipeline</b> consisting of the above two pipelines connected serially is made in order for smooth execution. 

![ADF pipeline](https://github.com/user-attachments/assets/ee7ce6b3-2b1d-4449-b341-e8bb057679ee)

### <u>6. Report Creation</u>

After the master pipeline is run, all the created Azure Databricks notebooks will be executed creating the data models which is then used for creating interactive reports in Power BI. In this project analysis is done seperately on Drivers and Constructors like finding Dominant drivers and constructors and more. The star schema developed for the project is shown below. 

![image](https://github.com/user-attachments/assets/4faa1ecc-0897-49ac-962e-5774fefae99a)

To create the data model, a connection between PowerBI and Azure Databricks Delta Lake Tables were made and the tables present in the Delta Lakes were imported into Power BI. The reports made from the above data model are shown below : 

![Driver Report](https://github.com/user-attachments/assets/94a6801f-dfa3-4b6b-8e26-68504b068b10)


![Constructor Report](https://github.com/user-attachments/assets/78b8769b-5c2f-44b8-b360-c256045d4470)

## <u>Conclusion</u>

This project demonstrates a practical approach to implementing an Azure-based data engineering solution. By leveraging Azure Data Factory, Databricks, Azure Data Lake Storage and PowerBI, the project showcases how to build a scalable, efficient, and production-ready data pipeline with inteactive reports to analyze the key metrics. I also welcome all to fork my repository and to suggest any changes or updates. You can also contact me via <a href="mailto:msgokul2011@gmail.com">e-mail</a> or my <a href="https://www.linkedin.com/in/gokul-manoharan/">LinkedIn</a>. 




