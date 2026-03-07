# 📊 Careplus Ticket Analytics Pipeline with AWS

## 📖 **Project Overview**

Careplus is a support system that logs tickets and system errors, capturing both structured support ticket data and unstructured log data. The project aims to create an **ETL pipeline** to process this data into actionable insights, providing metrics on ticket resolution, agent performance, and system health. Ultimately, these insights will be visualized using **Power BI** for efficient decision-making.

---

## 🎯 **Objectives**

- **Automate data ingestion** for Careplus support logs and tickets into **AWS S3**.
- **Process and clean data** using **AWS Lambda** and **AWS Glue**.
- **Generate actionable insights** for support tickets, agent performance, and system logs.
- Provide a **dashboard** to visualize key metrics, including:
  - Ticket count and resolution times
  - Agent performance
  - System health and log-level analysis
  - Predictive insights for ticket escalations

---

## 🧰 **Tools & Technologies**

| **Tool / Service**         | **Purpose**                                                                 |
|----------------------------|-----------------------------------------------------------------------------|
| **AWS S3**                 | Storage for raw and processed data                                           |
| **AWS Lambda**             | Serverless compute for data processing (ETL)                                 |
| **AWS Glue**               | Managed ETL service for data cleaning and transformation                      |
| **AWS RedShift**           | Data warehouse for storing processed data                                    |
| **Amazon Athena**          | Querying processed data for ad-hoc analysis                                  |
| **Power BI**               | Dashboard visualization of key metrics                                       |
| **Python**                 | Scripting for Lambda and Glue functions                                      |
| **Pandas**                 | Data manipulation and transformation                                         |

---

## 🧱 **Pipeline Architecture**

![Pipeline Architecture](Pipeline_architecture.jpeg)

### **Data Flow**:
1. **Support Logs & Tickets**: Data is ingested into **S3** in CSV and log file formats.
2. **ETL with AWS Lambda & Glue**:
   - **Lambda Functions** process raw data from **S3** (e.g., unstructured log data).
   - **Glue** processes raw data from **S3** (e.g., support ticket data in CSV format).
3. **Ad-hoc Analysis**: The data is queried using **Amazon Athena** for deeper insights.
4. **Data Warehouse**: Processed data is stored in **RedShift** for faster querying.
5. **Visualization**: Power BI connects to Redshift to provide real-time dashboards.

---

## 📊 **Power BI Reporting Layer**

Power BI connects to **Redshift** for visual insights.

![Insights Report](careplus_insights_report1.jpg)
![Insights Report](careplus_insights_report2.jpg)

---

## ✅ **Outcomes**

- 🏆 Real-time insights into ticket performance and agent productivity
- 🚀 Automated ETL pipeline for data processing and transformation
- 💡 Enhanced decision-making capabilities with actionable reports
- 🔐 Scalability and governance with **AWS Glue** and **Amazon RedShift**

