# 🧾 Big Data Project — Hadoop MapReduce

This repository contains two Hadoop MapReduce tasks that demonstrate different data processing techniques used in big data environments.

Each task is implemented in a separate folder and focuses on solving a specific problem using MapReduce concepts such as joining datasets, partitioning data, and computing aggregated results.

---

## 📌 Project Structure

```text
BigDataProject/
│
├── Employee Payroll Join using Hadoop MapReduce/
│   ├── src/
│   ├── input/
│   ├── output_sample/
│   └── README.md
│
├── Age Group Partitioning (Hadoop MapReduce)/
│   ├── src/
│   ├── input/
│   ├── output_sample/
│   └── README.md
│
└── README.md
```

---

## 🔹 Task 1 — Employee Payroll Join using Hadoop MapReduce

This task focuses on combining two different datasets:

* Employee data
* Payroll data

The goal is to produce a final dataset that includes employee information along with salary calculations.

### What this task does:

* Reads employee records and payroll records from separate files
* Matches records using `employeeId`
* Combines both datasets into one unified output
* Calculates total salary for each record
* Tracks the maximum salary for each employee
* Handles missing employee data if needed

### What is implemented:

* Mapper for employee data
* Mapper for payroll data
* Reducer to perform join and calculations
* Driver to run the MapReduce job

👉 For full implementation details, check the **Task 1 folder**.

---

## 🔹 Task 2 — Age Group Partitioning (Hadoop MapReduce)

This task focuses on analyzing demographic data based on different age groups.

The main idea is to distribute data across multiple reducers so that each reducer handles a specific age group.

### What this task does:

* Reads demographic records from the dataset
* Classifies each record into an age group
* Distributes data using a custom partitioner
* Sends each age group to a specific reducer
* Processes each group independently
* Calculates statistics such as income and employment metrics
* Produces organized output per age category

### What is implemented:

* Mapper to process input data
* Custom Partitioner to control data distribution
* Reducer to calculate statistics
* Driver to configure and run the job

👉 For full implementation details, check the **Task 2 folder**.

---

## 🚀 Project Purpose

This project demonstrates how Hadoop MapReduce can be used to:

* Process large-scale datasets efficiently
* Join multiple datasets
* Distribute data across reducers
* Perform parallel computations
* Generate meaningful insights from raw data

---

## 📖 Notes

* Each task is independent and can be run separately
* Each folder contains its own detailed README
* Sample input and output files are included for testing

---

## 👨‍💻 Author

This project is developed as part of a **Big Data / Hadoop MapReduce academic project**, focusing on practical implementation of distributed data processing techniques.
