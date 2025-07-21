
# 📊 Telecom Customer Churn Risk Analysis (PySpark + SQL)

This project demonstrates an end-to-end data pipeline and analytical workflow using **PySpark** and **SQL** to analyze **customer churn risk** in a telecom company. It includes data ingestion, cleaning, feature engineering, risk classification, and business-driven SQL analysis.

---

## 🚀 Project Highlights

- 🔄 **Data Ingestion & Transformation** using PySpark
- 🧼 **Data Cleaning** and null handling
- 🔗 **Joins across multiple data sources** (customers, usage, complaints)
- 📊 **Churn Risk Classification** based on usage patterns and complaints
- 📈 **SQL-based Business Analysis** on churn drivers
- ✅ Structured, modular notebooks with markdown explanations

---

## 📁 Project Structure

```
telecom-churn-risk-pyspark/
├── notebooks/
│   ├── 1_data_transformation.ipynb     ← ETL & churn risk logic
│   └── 2_churn_sql_analysis.ipynb      ← SQL insights on churn data
├── data/
│   └── churn_final.csv                 ← Cleaned, labeled dataset
├── README.md
```

---

## 📊 Dataset Overview

We used synthetic telecom data including:

- **Customers Data** (`customer_id`, `age`, `plan_type`, `city`, `join_date`)
- **Usage Data** (`calls_made`, `data_used_gb`, `month`)
- **Complaints Data** (`complaint_type`, `complaint_date`)

All data was cleaned, joined, and enriched in PySpark.

---

## 🧠 Churn Risk Logic

Each customer was labeled as **Low**, **Medium**, or **High** churn risk based on:

- Average monthly **calls** and **data usage**
- Number of **complaints**
- Plan type and tenure

The risk logic was implemented using `when` conditions and feature engineering in PySpark.

---

## ❓ Sample SQL Business Questions Answered

- What is the overall churn risk distribution?
- Does age impact churn risk?
- Which cities have the most high-risk customers?
- What plan types are most vulnerable to churn?
- Does customer usage pattern correlate with churn?

📌 All SQL queries are run via `createOrReplaceTempView()` in Spark.

---

## 💻 Tech Stack

- 🔥 **PySpark** for data transformation & feature engineering
- 🧪 **Spark SQL** for business analysis
- 📄 CSV-based input/output
- 📝 Jupyter Notebooks for code & documentation

---

## 📚 Future Enhancements

- Add interactive dashboards using Power BI or Tableau
- Apply ML models for churn **prediction** (Logistic Regression, XGBoost)
- Automate the pipeline using Apache Airflow or ADF

---

## 🙋‍♀️ About Me

👩‍💻 I’m Anjali Mane, a Data Engineer with experience in Azure Data Factory, Synapse Analytics, and Python. I built this project to demonstrate practical data transformation and analysis skills.

📬 Let’s connect on [LinkedIn](https://www.linkedin.com/in/your-link/)  
🌟 If you found this helpful, feel free to star the repo!
