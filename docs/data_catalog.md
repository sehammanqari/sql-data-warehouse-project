# Gold Layer Data Catalog

## Overview

The **Gold Layer** represents the business-ready data model of the data warehouse.
It is designed to support **analytics, dashboards, and reporting use cases**.

The Gold layer transforms the cleaned data from the **Silver layer** into a **star schema structure**, consisting of:

* **Dimension tables**: descriptive attributes for business entities.
* **Fact tables**: measurable business events.

This structure improves performance and simplifies analytical queries.

---

# Gold Layer Tables

| Table Name           | Type      | Description                                               |
| -------------------- | --------- | --------------------------------------------------------- |
| `gold.dim_customers` | Dimension | Contains customer demographic and geographic information. |
| `gold.dim_products`  | Dimension | Contains product details and classification attributes.   |
| `gold.fact_sales`    | Fact      | Contains transactional sales data used for analytics.     |

---

# 1. `gold.dim_customers`

### Purpose

Stores customer information enriched with demographic and geographic attributes.

### Columns

| Column Name       | Data Type    | Description                                                                     |
| ----------------- | ------------ | ------------------------------------------------------------------------------- |
| `customer_key`    | INT          | Surrogate key uniquely identifying each customer record in the dimension table. |
| `customer_id`     | INT          | Unique identifier assigned to the customer in the source system.                |
| `customer_number` | NVARCHAR(50) | Business identifier used to track the customer.                                 |
| `first_name`      | NVARCHAR(50) | Customer's first name.                                                          |
| `last_name`       | NVARCHAR(50) | Customer's last name or family name.                                            |
| `country`         | NVARCHAR(50) | Country where the customer resides.                                             |
| `marital_status`  | NVARCHAR(50) | Customer marital status (e.g., Single, Married, n/a).                           |
| `gender`          | NVARCHAR(50) | Customer gender (e.g., Male, Female, n/a).                                      |
| `birthdate`       | DATE         | Customer date of birth.                                                         |
| `create_date`     | DATE         | Date when the customer record was created in the system.                        |

---

# 2. `gold.dim_products`

### Purpose

Provides descriptive information about products and their classification attributes.

### Columns

| Column Name            | Data Type    | Description                                                    |
| ---------------------- | ------------ | -------------------------------------------------------------- |
| `product_key`          | INT          | Surrogate key uniquely identifying each product record.        |
| `product_id`           | INT          | Unique product identifier from the source system.              |
| `product_number`       | NVARCHAR(50) | Alphanumeric identifier used to reference the product.         |
| `product_name`         | NVARCHAR(50) | Name of the product.                                           |
| `category_id`          | NVARCHAR(50) | Identifier for the product category.                           |
| `category`             | NVARCHAR(50) | High-level product classification.                             |
| `subcategory`          | NVARCHAR(50) | More detailed classification of the product.                   |
| `maintenance_required` | NVARCHAR(50) | Indicates whether the product requires maintenance (Yes / No). |
| `cost`                 | INT          | Base cost of the product.                                      |
| `product_line`         | NVARCHAR(50) | Product family or line (e.g., Mountain, Road, Touring).        |
| `start_date`           | DATE         | Date when the product became available for sale.               |

---

# 3. `gold.fact_sales`

### Purpose

Stores transactional sales data used for business analytics and reporting.

### Columns

| Column Name     | Data Type    | Description                              |
| --------------- | ------------ | ---------------------------------------- |
| `order_number`  | NVARCHAR(50) | Unique identifier for each sales order.  |
| `product_key`   | INT          | Foreign key referencing `dim_products`.  |
| `customer_key`  | INT          | Foreign key referencing `dim_customers`. |
| `order_date`    | DATE         | Date when the order was placed.          |
| `shipping_date` | DATE         | Date when the order was shipped.         |
| `due_date`      | DATE         | Date when payment is due.                |
| `sales_amount`  | INT          | Total monetary value of the sale.        |
| `quantity`      | INT          | Number of units sold.                    |
| `price`         | INT          | Price per unit of the product.           |

---

# Data Model Relationships

The Gold layer follows a **Star Schema** design.

```
           dim_customers
                |
                |
                |
dim_products ---- fact_sales
```

### Relationships

| Fact Table   | Dimension       | Key            |
| ------------ | --------------- | -------------- |
| `fact_sales` | `dim_customers` | `customer_key` |
| `fact_sales` | `dim_products`  | `product_key`  |

In a star schema, the relationship between fact and dimensions is 1-to-many (1:N).

---

# Analytical Use Cases

This Gold layer enables the following business analyses:

* Customer purchasing behavior
* Product performance analysis
* Sales trend analysis
* Revenue reporting
* Customer segmentation
* Product category sales analysis

---

# Notes

* The **Gold layer is optimized for analytics**, not operational workloads.
* Data is sourced from the **Silver layer**, which already contains cleaned and standardized data.
* Surrogate keys (`customer_key`, `product_key`) improve join performance for analytical queries.
