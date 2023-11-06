{% docs __overview__ %}
# DBT Project Overview

## Welcome to Bluwalk Analytics Hub!
This repository serves as the foundation for our data transformation efforts using DBT. Below is a comprehensive guide to understanding its structure and content.

---

### Introduction
At Bluwalk, we utilize DBT to transform raw data into actionable and trustworthy datasets that power our analytics and inform business decisions.

---

### Project Structure

- **Models**: The heart of our DBT project. Contains SQL transformations that produce our analytics tables and views.

---

### Key Models

Our models are organized into distinct categories:

- **Staging**:
Staging models provide a foundational transformation layer. Should source data undergo changes, adjustments at the staging level ensure downstream consistency without manual overhauls. Staging models clean and standardize raw data from the warehouse, ensuring uniformity in downstream models. 
  - Static Variant (`staging`): Provides fast data accessibility through tables with 1 day freshness.
  - Real-time Variant (`staging_rt`): Provides real-time data accessibility through views.

- **Marts**:
This layer encourages complexity to meet business needs. Data mart models, characterized by intricate transformations, embed business logic. They craft the essential data assets utilized in downstream analysis.
  - Static Variant (`marts`): Provides fast data accessibility through tables with 1 day freshness.
  - Real-time Variant (`marts_rt`): Provides real-time data accessibility through views.

- **Reports**:
  - Static Variant (`report`): Provides fast data accessibility through tables with 1 day freshness.
  - Real-time Variant (`report_rt`): Provides real-time data accessibility through views.


---

## Naming Conventions

### Prefix: 
- **Staging**: Models that reside in a 'staging' or 'staging_rt' folder, are prefixed with `stg_` (e.g., `stg_zendesk_chats` reflecting the raw `zendesk.chats` table).
- **Marts**: Models inside the 'marts' or 'marts_rt' folder, models are often dimension and fact tables, prefixed with `dim_` or `fct_`.
- **Report**: Models inside the 'report' or 'report_rt' folder, models are reporting tables, prefixed with `rpt_`.

### Difference between Facts (fct_) and Dimensions (dim_): 
- **Facts**: Fact tables primarily contain the measures or metrics of the business, often numeric data that you would want to analyze. This could include sales amounts, quantities sold, profit margins, etc.
- **Dimensions**: Dimension tables contain descriptive, textual or categorical information, often referred to as "attributes", which are typically the entry points to data. Examples include dates, product names, geographical locations, or customer names.


---

### DBT Documentation

For in-depth details on models, relationships, lineage, and more, make use of our auto-generated DBT documentation. This resource provides insights into every transformation, ensuring transparency and trust in our data processes.

---

### Questions or Feedback?

If you have questions about a specific model, please refer first to its dedicated documentation. For broader inquiries or feedback, contact our Data Team:

- 📧 Email: [data-team-email@example.com]
- 💬 Slack: `#data-team`

Remember to replace placeholders like `[Your Company Name]` with actual details relevant to your organization.


{% enddocs %}

{% docs __dbt_utils__ %}
# Utility macros
Our dbt project heavily uses this suite of utility macros, especially:
- `surrogate_key`
- `test_equality`
- `pivot`
{% enddocs %}

{% docs __snowplow__ %}
# Snowplow sessionization
Our organization uses this package of transformations to roll Snowplow events
up to page views and sessions.
{% enddocs %}