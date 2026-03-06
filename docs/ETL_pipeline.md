# ETL Pipeline

## Overview

The data warehouse uses an **ETL (Extract, Transform, Load) pipeline** that moves data through the Bronze, Silver, and Gold layers.

```
Source Systems
     |
     v
Bronze Layer
(raw ingestion)
     |
     v
Silver Layer
(data cleansing & transformation)
     |
     v
Gold Layer
(analytics-ready model)
```

---

# ETL Process Steps

## 1. Extract

Data is extracted from **source CSV datasets** representing CRM and ERP systems.

Sources include:

```
CRM System
ERP System
```

Files are loaded into SQL Server using:

```sql
BULK INSERT
```

---

## 2. Transform

During the **Silver layer processing**, several transformations occur:

* Data cleaning
* Standardizing gender and marital status
* Fixing negative or null values
* Deriving missing sales metrics
* Removing duplicate customer records
* Splitting product keys into category attributes

These transformations improve **data quality and consistency**.

---

## 3. Load

The cleaned data is loaded into the **Gold layer views**, where it is structured into a **star schema model**.

The Gold layer provides:

* business-friendly column names
* integrated customer data
* product hierarchy
* sales metrics


