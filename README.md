# Washington State Electric Vehicles ETL Pipeline

## Table of Contents
1. [Overview](#overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Data Source](#data-source)
4. [ETL Workflow](#etl-workflow)
5. [Setup and Getting Started](#setup-and-getting-started)
6. [Components](#components)
7. [Prerequisites](#prerequisites)
8. [Running the Pipeline](#running-the-pipeline)
11. [Data Visualization with Tableau Desktop](#data-visualization-with-tableau-desktop)
12. [Contribution](#contribution)

## Overview
This ETL pipeline processes and analyzes electric vehicle population data for Washington State. It is designed to extract raw data from an S3 bucket, transform it using Apache Spark on an EMR cluster, and load the cleaned data into Snowflake for analysis.

## Architecture Diagram
![Architecture_Diagram](https://github.com/user-attachments/assets/ad13eefe-63dd-4a39-b87b-fa08fc2724e5)
The pipeline architecture involves:
- **Amazon EC2**: Hosts the Airflow environment, set up using Docker and Astronomer.
- **Amazon S3**: Stores both raw and transformed data.
- **AWS EMR**: Executes the Spark scripts for data transformation.
- **Snowflake**: Stores the final cleaned and structured data.
- **Apache Airflow**: Manages and schedules the ETL workflow.
- **Tableau**: Visualizes the final data for analysis.

## Data Source
The raw data for this project is sourced from the [Electric Vehicle Population Data](https://catalog.data.gov/dataset/electric-vehicle-population-data) available on Data.gov. This dataset provides a comprehensive record of electric vehicles registered in Washington State, including information such as vehicle make, model, year, etc.

This data is critical for understanding trends in electric vehicle adoption, infrastructure needs, and environmental impacts. The dataset is provided in CSV format and is stored in an Amazon S3 bucket for processing within the ETL pipeline.

## ETL Workflow
1. **Data Extraction**: Raw data is extracted from the S3 bucket.
2. **Data Transformation**: A Spark script cleans, standardizes, and transforms the raw CSV data into a set of fact and dimension tables, which are then converted into Parquet format for efficient storage and processing.
3. **Data Loading**: The transformed data is loaded into Snowflake.
4. **Data Visualization**: A Tableau dashboard is created to visualize the data for analysis.

## Setup and Getting Started
To run this project, follow the steps below:

1. **Install Required Tools**: Ensure you have Visual Studio Code, Docker Desktop, and the Astro CLI installed.
2. **Set Up the Environment**: Create an EC2 instance on AWS, SSH into it, and set up the Airflow environment using Docker and Astronomer.
3. **Prepare Data Storage**: Create an S3 bucket with the necessary directory structure for storing raw and transformed data.
4. **Initialize Airflow**: Use Astro CLI to initialize and start the Airflow environment.
5. **Configure Snowflake**: Set up Snowflake for data loading and create the necessary tables.
6. **Run the ETL Pipeline**: Trigger the Airflow DAG to execute the ETL process, transforming and loading the data.
7. **Visualize the Data**: Connect Tableau to Snowflake and create a dashboard to analyze the data.

For detailed setup and execution instructions, refer to the `how_to_run.docx` document.

## Components
- **Airflow DAG**: Manages the overall workflow, including the creation and termination of EMR clusters, execution of Spark jobs, and loading data into Snowflake.
- **Spark Script**: Transforms the raw data, including data cleaning, standardization, and structuring it into fact and dimension tables in Parquet format.
- **Snowflake SQL**: Executes SQL commands to load the transformed data into Snowflake.
- **Tableau Dashboard**: Visualizes the cleaned data stored in Snowflake.

## Prerequisites
- **Visual Studio Code**: For SSH access and project management.
- **Astro CLI**: To manage the Airflow environment within Docker.
- **Docker Desktop**: To containerize the Airflow environment.
- **AWS Account**: Required for EC2, S3, and EMR services.
- **Snowflake Account**: For storing and querying the final transformed data.
- **Tableau**: For visualizing the final data stored in Snowflake.

## Running the Pipeline
1. **Environment Setup**: Set up the environment as described in `how_to_run.docx`, including creating an EC2 instance and configuring IAM roles.
2. **Airflow Initialization**: Initialize the Airflow environment using Astro CLI and Docker.
3. **Data Storage**: Prepare the S3 bucket structure for storing raw and transformed data.
4. **Data Transformation**: Set up the Spark transformation scripts to clean and convert the CSV data into fact and dimension tables in Parquet format, and run them on an EMR cluster.
5. **Data Loading**: Configure Snowflake for data loading and execute the SQL scripts to store the Parquet data.
6. **Data Visualization**: Create a Tableau dashboard to visualize the data stored in Snowflake.
7. **DAG Execution**: Start the Airflow components and trigger the DAG to execute the ETL process.

## Data Visualization with Tableau Desktop
Once the data is loaded into Snowflake:
1. **Install the ODBC Snowflake Driver**: Required for Tableau Desktop to connect to Snowflake.
2. **Create an Extract Connection**: Connect Tableau Desktop to Snowflake.
3. **Data Modeling in Tableau**: Create a star schema and define relationships between the cleaned data tables.
4. **Create Visualizations**: Use Tableau Desktop to process and visualize the data.

## Dashboard
[![Washingtonstate_Animation](https://github.com/user-attachments/assets/66ceab9f-af6e-4e58-afe5-b4c7227d4d05)
](https://public.tableau.com/app/profile/ravi.shankar.p.r/viz/ElectricvehiclestatsinWashingtonState/Home)

> ### Checkout Tableau data visualization at [Electric vehicle stats in Washington State \| Tableau Public](https://public.tableau.com/app/profile/ravi.shankar.p.r/viz/ElectricvehiclestatsinWashingtonState/Home)

> ### Checkout [how_to_run.docx](https://github.com/ravishankar324/Washington-state-electric-vehicles-ETL-pipeline/blob/main/how_to_run.docx) file for detailed steps to run this project.

## Contribution
Contributions are welcome! Please open an issue or submit a pull request with any improvements.
