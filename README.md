<!DOCTYPE html>
<html lang="en">
<body>

<h1>Automated_ETL_Pipeline_Using_Airflow</h1>

<h2>Summary</h2>
<p>
    This project sets up an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process and analyze real estate data from RedFin. 
    The data is extracted from a specified URL, transformed, and loaded into AWS S3 and AWS Redshift for further analysis. 
    The key steps of the ETL process are:
</p>
<ul>
    <li><strong>Extract:</strong> Extracts data from a specified URL containing RedFin real estate market data.</li>
    <li><strong>Transform:</strong> Transforms the extracted data by cleaning it, converting date formats, and removing unnecessary characters.</li>
    <li><strong>Load:</strong> Loads the transformed data into AWS S3 and then transfers it from S3 to AWS Redshift for further analysis.</li>
</ul>

<h2>Tech</h2>
<ul>
    <li><strong>Python:</strong> Used for scripting the extraction and transformation processes.</li>
    <li><strong>AWS S3:</strong> Used for storing raw and transformed data.</li>
    <li><strong>AWS Redshift:</strong> Used as the data warehouse to load and analyze the transformed data.</li>
    <li><strong>AWS EC2:</strong> Used for Virtual Server, Linux.</li>
    <li><strong>Airflow:</strong> Used for orchestrating the ETL pipeline.</li>
  
</ul>

<h2>Working of the Code</h2>
<p>
    The ETL pipeline is defined using an Airflow DAG with the following tasks:
</p>
<ul>
    <li><strong>Extract Data (extract_redfin_data):</strong> 
        This task reads data from the specified URL, saves it as a CSV file with a timestamped filename, uploads the raw data to an S3 bucket, 
        and returns the file path and filename.
    </li>
    <li><strong>Transform Data (transform_redfin_data):</strong> 
        This task retrieves the extracted data, performs necessary transformations such as removing commas from city names, converting date formats, 
        and filtering columns, saves the transformed data locally, and returns the transformed file path.
    </li>
    <li><strong>Load Transformed Data to S3 (load_to_s3):</strong> 
        This task uploads the transformed data to the transformed data S3 bucket using a bash command.
    </li>
    <li><strong>Transfer Data from S3 to Redshift (transfer_s3_to_redshift):</strong> 
        This task transfers the transformed data from the S3 bucket to a Redshift table using the S3ToRedshiftOperator.
    </li>
</ul>

<h2>DAG Representation</h2>
<p>
    Below is the representation of the DAG tasks and their dependencies:
</p>
<pre>
<code>extract_redfin_data --> transform_redfin_data --> load_to_s3 --> transfer_s3_to_redshift</code>
</pre>

</body>
</html>
