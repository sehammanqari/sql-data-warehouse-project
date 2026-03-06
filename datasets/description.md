# Datasets

## Overview

This folder contains the **raw source datasets** used in the data warehouse project.
The data represents information coming from two operational systems:

* **CRM System** – customer, product, and sales information
* **ERP System** – customer demographics, location data, and product category attributes

These datasets are used as the **input for the Bronze layer**, where the data is ingested into the data warehouse without modification.

The data is then processed through the following pipeline:

```
Source Files → Bronze Layer → Silver Layer → Gold Layer
```

---

# Dataset Structure

## CRM Source Data

Location:

```
datasets/source_crm/
```

Files:

| File                | Description                                                                                                    |
| ------------------- | -------------------------------------------------------------------------------------------------------------- |
| `cust_info.csv`     | Contains customer information such as names, gender, marital status, and creation date.                        |
| `prd_info.csv`      | Contains product master data including product identifiers, cost, category keys, and product line information. |
| `sales_details.csv` | Contains transactional sales data including order numbers, product IDs, customer IDs, quantities, and prices.  |

---

## ERP Source Data

Location:

```
datasets/source_erp/
```

Files:

| File              | Description                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------------- |
| `CUST_AZ12.csv`   | Contains additional customer demographic data such as birthdate and gender.                       |
| `LOC_A101.csv`    | Contains customer location data including country information.                                    |
| `PX_CAT_G1V2.csv` | Contains product category metadata including category, subcategory, and maintenance requirements. |

---

# Usage

These datasets are loaded into the data warehouse using **SQL Server BULK INSERT operations** as part of the **Bronze layer ingestion process**.

Example:

```sql
BULK INSERT bronze.crm_cust_info
FROM 'datasets/source_crm/cust_info.csv'
```

The raw data is then transformed and cleaned in the **Silver layer**, and finally modeled for analytics in the **Gold layer**.

