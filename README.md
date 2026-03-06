# Data Warehouse and Analytics Project

This project implements a **modern SQL-based data warehouse** using a layered architecture (Bronze, Silver, Gold) to transform raw operational data into a structured analytical model.

The solution demonstrates **end-to-end data warehousing practices**, including data ingestion, transformation, dimensional modeling, and data quality validation.

The final warehouse model enables business analysis related to **customers, products, and sales performance**.

---

# Data Architecture

The project follows a **Medallion Architecture** design pattern consisting of three layers:

* **Bronze Layer** – raw data ingestion from source systems
* **Silver Layer** – cleaned and standardized data
* **Gold Layer** – business-ready dimensional model for analytics

Data flows through the warehouse as follows:

```
Source Systems
     │
     ▼
Bronze Layer
(raw data ingestion)
     │
     ▼
Silver Layer
(data cleaning & transformation)
     │
     ▼
Gold Layer
(star schema for analytics)
```

---

# Project Requirements

## Building the Data Warehouse (Data Engineering)

### Objective

Design and implement a modern data warehouse using **SQL Server** to consolidate data into an analytical model that supports reporting and decision-making.

### Specifications

* **Data Sources:** Import structured data from multiple source systems.
* **Data Quality:** Clean and prepare data before loading into the warehouse.
* **Integration:** Transform raw data into a unified **star schema** model.
* **Scope:** Focus on building a clean, analytics-ready dataset.
* **Documentation:** Provide clear explanations of the data model and architecture.

---

# Analytics & Reporting (Data Analytics)

### Objective

Develop SQL-based analytical queries to generate insights related to:

* **Customer Behavior**
* **Product Performance**
* **Sales Trends**

These insights help stakeholders understand key metrics and support data-driven decision-making.

---

# Data Model

The **Gold layer** follows a **Star Schema** design.

It includes:

### Dimension Tables

* `gold.dim_customers`
* `gold.dim_products`

Dimension tables store **descriptive attributes** about business entities.

### Fact Table

* `gold.fact_sales`

The fact table stores **transactional sales data** and connects to dimensions using surrogate keys.

```
           dim_customers
                 │
                 │
dim_products ─── fact_sales
```

This model is optimized for **analytical queries and reporting**.

---

# Project Structure

```
sql-data-warehouse-project
│
├── datasets
│   ├── source_crm
│   └── source_erp
    └── description.md
│
├── scripts 
│   ├── bronze
│   │   ├── ddl_bronze.sql
│   │   └── stored_proc_load_bronze.sql
│   │
│   ├── silver
│   │   ├── ddl_silver.sql
│   │   ├── stored_proc_load_silver.sql
│   │   
│   │
│   └── gold
│   │   └── ddl_gold.sql
│   │ 
│   └──  init_database.sql
│
├── tests
│   ├── quality_checks_silver.sql
│   └── quality_checks_gold.sql
│
├── docs
│   ├── ETL_pipline.md
│   └── data_architecute.md
│   └── data_catalog.md
│   └── naming_convention.md
│   └── star_schema_design.md
│
├── README.md
└── LICENSE
```

---

# Data Quality Checks

Quality validation scripts ensure the integrity and reliability of the warehouse.

The checks include:

* **Uniqueness validation** for surrogate keys in dimension tables
* **Referential integrity checks** between fact and dimension tables
* **Data model relationship validation**

Example checks include:

* Duplicate key detection
* Fact-to-dimension connectivity validation

These tests help ensure the warehouse produces **reliable analytical results**.

---

# Tools & Technologies

This project was built using:

* **Microsoft SQL Server**
* **SQL Server Management Studio (SSMS)**
* **T-SQL**
* **Git & GitHub** for version control and project documentation

---

# Key Skills

This project demonstrates practical skills in:

* Data Warehouse Architecture
* Star Schema Data Modeling
* SQL Data Transformation
* ETL Pipeline Development
* Data Quality Validation
* Analytical Query Design
* Data Documentation and Repository Organization

---

# About Me

Hi, I’m **Seham Hafez Manqari**, a Data Science student focused on developing strong foundations in **SQL, data analytics, and data engineering concepts**.

This project represents part of my journey toward becoming a professional **Data Analyst**, where I focus on building practical projects that demonstrate real-world data skills.

---
📧 Email: [sehammanqari@gmail.com](mailto:sehammanqari@gmail.com)

