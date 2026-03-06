# Naming Conventions

## Overview

This project follows a consistent naming convention to improve **readability, maintainability, and clarity** across the data warehouse layers.

The conventions apply to:

* Schemas
* Tables
* Columns
* Keys
* Fact and Dimension objects

---

# Schema Naming

Schemas represent the **data warehouse layers**.

| Schema   | Description                                                       |
| -------- | ----------------------------------------------------------------- |
| `bronze` | Raw data ingested directly from source systems.                   |
| `silver` | Cleaned and standardized data after data quality transformations. |
| `gold`   | Business-ready analytical model used for reporting and analytics. |

Example:

```sql
bronze.crm_cust_info
silver.crm_cust_info
gold.dim_customers
```

---

# Table Naming

Tables follow different naming styles depending on the **layer**.

### Bronze Layer

Bronze tables preserve **source system naming**.

Format:

```
<source_system>_<table_name>
```

Examples:

```
crm_cust_info
crm_prd_info
crm_sales_details
erp_cust_az12
erp_loc_a101
erp_px_cat_g1v2
```

Purpose: maintain traceability to the original source systems.

---

### Silver Layer

Silver tables retain the **same names as Bronze**, but the data is:

* cleaned
* standardized
* validated

Example:

```
silver.crm_cust_info
silver.crm_prd_info
silver.crm_sales_details
```

---

### Gold Layer

Gold tables follow **analytics-friendly naming** using a **star schema convention**.

| Prefix  | Meaning         |
| ------- | --------------- |
| `dim_`  | Dimension table |
| `fact_` | Fact table      |

Examples:

```
gold.dim_customers
gold.dim_products
gold.fact_sales
```

---

# Column Naming

Columns follow **snake_case** format:

```
lowercase_words_separated_by_underscores
```

Example:

```
customer_key
product_number
sales_amount
order_date
shipping_date
```

This improves readability and consistency.

---

# Key Naming

The project uses **clear naming conventions for keys**.

### Surrogate Keys (Dimensions)

Format:

```
<entity>_key
```

Examples:

```
customer_key
product_key
```

These are generated in the **Gold layer** and used for analytics joins.

---

### Business Keys

Format:

```
<entity>_id
```

Examples:

```
customer_id
product_id
```

These come from the **source systems**.

---

### Reference Numbers

Format:

```
<entity>_number
```

Examples:

```
order_number
product_number
customer_number
```

These represent business identifiers used for tracking.

---

# Date Columns

Date columns follow a consistent format:

```
<event>_date
```

Examples:

```
order_date
shipping_date
due_date
start_date
create_date
```

---

# Measure Columns

Numeric metrics in fact tables use descriptive names.

Examples:

```
sales_amount
quantity
price
cost
```

---

# Prefixes Used in Source Tables

Source tables contain abbreviations derived from the **original system fields**.

Examples:

| Prefix | Meaning  |
| ------ | -------- |
| `cst_` | Customer |
| `prd_` | Product  |
| `sls_` | Sales    |

Examples:

```
cst_firstname
prd_cost
sls_quantity
```

These prefixes were preserved in the **Bronze and Silver layers** to maintain lineage with the source systems.

---

# Summary

This naming convention ensures:

* Clear **data lineage across Bronze → Silver → Gold**
* Consistent **schema organization**
* Easy understanding of **fact vs dimension tables**
* Readable and maintainable SQL code

