# Data Architecture

## Overview

This project implements a **modern data warehouse architecture** using a **multi-layered design** to ensure data quality, scalability, and analytical performance.

The architecture follows a **Medallion Architecture pattern** consisting of three layers:

* **Bronze Layer** – Raw data ingestion
* **Silver Layer** – Cleaned and standardized data
* **Gold Layer** – Business-ready analytical model

Each layer has a specific responsibility in transforming raw operational data into structured data optimized for analytics.

---

# Data Warehouse Layers

## Bronze Layer – Raw Data

The **Bronze layer** stores raw data ingested directly from the source systems.

Characteristics:

* Data is stored **exactly as received from the source**
* Minimal transformation is applied
* Preserves **original schema and naming**
* Used as the **single source of truth**

Example tables:

```
bronze.crm_cust_info
bronze.crm_prd_info
bronze.crm_sales_details
bronze.erp_cust_az12
bronze.erp_loc_a101
bronze.erp_px_cat_g1v2
```

Data in this layer is loaded using **BULK INSERT** operations from CSV files.

---

## Silver Layer – Cleaned Data

The **Silver layer** contains **validated and standardized data** prepared from the Bronze layer.

Key transformations include:

* Data cleansing
* Removing duplicates
* Standardizing categorical values
* Fixing inconsistent data
* Handling null or invalid values
* Data integration between CRM and ERP systems

Example tables:

```
silver.crm_cust_info
silver.crm_prd_info
silver.crm_sales_details
silver.erp_cust_az12
silver.erp_loc_a101
silver.erp_px_cat_g1v2
```

This layer ensures the dataset is **clean, consistent, and reliable** before being used for analytics.

---

## Gold Layer – Business Model

The **Gold layer** represents the **business-level analytical model**.

It is designed using a **Star Schema** to optimize analytical queries and reporting.

Structure:

* **Dimension tables** for descriptive attributes
* **Fact tables** for measurable events

Example views:

```
gold.dim_customers
gold.dim_products
gold.fact_sales
```

These views combine data from the Silver layer to create a **business-friendly schema** used for analytics and reporting.
