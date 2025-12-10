# sql_data_warehouse_project
Build a SQL-based Data Warehouse with Oracle server, including ETL processes, for storing, transforming, and analyzing structured business data.

📦 Data Warehouse Project
📘 Overview

This project demonstrates my understanding of how a Data Warehouse operates, including its architecture, data modeling principles, and ETL processes.
I implemented the three-layered data warehouse approach (Bronze → Silver → Gold) and applied dimensional modeling using Star Schema and Snowflake Schema.
To simplify the explanation and improve clarity, I created schematic visualizations of the data flows and model structures.

🏗️ Architecture
Bronze Layer

Raw data ingestion

Minimal transformation

Data stored as-is for full traceability

Silver Layer

Data cleaning & standardization

Deduplication, type corrections, normalization

Relationships between entities established

Gold Layer

Business-ready curated tables

Consists of Fact and Dimension tables

Used for analytics, BI reporting, dashboards

📊 Data Modeling

The project uses two main dimensional modeling techniques:

⭐ Star Schema

Central fact table

Connected dimension tables

Simple and fast for analytical queries

❄️ Snowflake Schema

Normalized version of dimensions

Reduces redundancy

More structured but slightly more complex

🔄 ETL Process

The ETL pipeline in this project includes:

Extracting raw data

Cleaning, transforming, validating

Loading into appropriate layers

Final modeling into facts and dimensions

ETL flow:
Source → Bronze → Silver → Gold → BI

🛠️ Tools & Technologies

SQL

Data modeling (Star/Snowflake)

Data Warehouse architecture

Schematic visualization tools (diagrams)

📚 What I Learned

How a Data Warehouse is structured

How ETL pipelines work end-to-end

Benefits of multi-layered architecture

Designing fact/dimension models

Visualizing data flows and schemas

📌 Future Improvements

Add more complex transformations

Automate ETL pipeline

Include BI dashboard examples
