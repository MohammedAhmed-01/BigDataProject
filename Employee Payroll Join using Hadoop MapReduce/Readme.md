# 🧾 Employee Payroll Join using Hadoop MapReduce

---

# 📌 1. Project Overview

This project implements a **Reduce-Side Join** using **Hadoop MapReduce (Java)** to integrate and process two heterogeneous datasets:

* Employee Directory Dataset
* Monthly Payroll Transactions Dataset

The system enriches payroll records with employee information and computes key financial metrics such as:

* Total Pay per payroll record
* Maximum Pay per employee across all months

The solution is designed to be:

* ✅ Scalable (supports datasets larger than 1GB)
* ✅ Robust (handles malformed and missing records)
* ✅ Modular (separate Mapper, Reducer, and Driver components)
* ✅ Distributed and fault tolerant using Hadoop MapReduce

---

# 🎯 2. Problem Statement

The project processes two datasets:

## 🧑 Dataset 1 — Employee Directory

Contains static employee information.

## 💰 Dataset 2 — Payroll Transactions

Contains monthly payroll records including salary and bonus information.

---

## 🧩 Objective

For every payroll transaction:

1. Join payroll data with employee data using `employeeId`
2. Compute:

```text
totalPay = baseSalary + bonus
```

3. Track the highest salary paid to the employee across all months:

```text
maxPay
```

4. Generate enriched payroll output records.

---

# 📂 3. Input Data Specification

## 🧑 3.1 Employee File

### 📍 File Path

```bash
Data/employees.csv
```

### 📑 Schema

```text
employeeId,firstName,lastName,department
```

### ✅ Example

```text
EMP01,Nour,Hassan,Engineering
EMP02,Sara,Ali,HR
```

---

## 💰 3.2 Payroll File

### 📍 File Path

```bash
Data/payroll.csv
```

### 📑 Schema

```text
payrollId,employeeId,month,baseSalary,bonus
```

### ✅ Example

```text
PR001,EMP01,Jan,8000,500
PR002,EMP01,Feb,8200,500
```

---

# 📤 4. Output Specification

## 📑 Output Schema

```text
employeeId fullName,department,month,totalPay,maxPay
```

---

## ✅ Example Output

```text
EMP01 Nour Hassan,Engineering,Jan,8500,8700
EMP01 Nour Hassan,Engineering,Feb,8700,8700
EMP02 Sara Ali,HR,Jan,7300,7300
EMP04 UNKNOWN EMPLOYEE,UNKNOWN,Jan,6200,6200
```

---

# 🏗️ 5. Project Structure

```text
/media/sf_EmployeePayroll
│
├── Data
│   ├── employees.csv
│   └── payroll.csv
│
├── Src
│   ├── EmployeeMapper.java
│   ├── PayrollMapper.java
│   ├── PayrollReducer.java
│   └── PayrollJoinDriver.java
│
├── payroll_output
│
└── README.md
```

---

# ⚙️ 6. System Design & Architecture

This project follows the **Reduce-Side Join Pattern** in Hadoop MapReduce.

---

## 🔹 6.1 Data Flow

1. Employee and payroll datasets are read independently.
2. Each dataset is processed using a dedicated Mapper.
3. Intermediate records are grouped using `employeeId` as the key.
4. Hadoop Shuffle & Sort sends all related records to the same reducer.
5. Reducer performs join operations and salary aggregation.

---

## 🔹 6.2 Map Phase

### 🧑 EmployeeMapper

Responsibilities:

* Read employee records
* Extract employee information
* Emit tagged employee values

### 📤 Output

```text
key = employeeId
value = emp~firstName,lastName,department
```

---

### 💰 PayrollMapper

Responsibilities:

* Read payroll transactions
* Validate salary fields
* Emit tagged payroll values

### 📤 Output

```text
key = employeeId
value = pay~month,baseSalary,bonus
```

---

## 🔹 6.3 Shuffle & Sort Phase

Hadoop automatically:

* Groups all values by `employeeId`
* Transfers related records to the same reducer
* Sorts intermediate keys internally

---

## 🔹 6.4 Reduce Phase — PayrollReducer

The reducer performs four main operations.

---

### Step 1 — Data Separation

Separate:

* Employee record (`emp~`)
* Payroll records (`pay~`)

---

### Step 2 — Data Enrichment

Construct:

```text
fullName = firstName + lastName
```

Retrieve:

```text
department
```

---

### Step 3 — Salary Aggregation

For every payroll transaction:

```text
totalPay = baseSalary + bonus
```

Track:

```text
maxPay = maximum(totalPay)
```

across all months for the same employee.

---

### Step 4 — Final Output Generation

Emit one enriched output record for every payroll transaction.

---

# 🧠 7. Hadoop Features Used

| Feature               | Purpose                                         |
| --------------------- | ----------------------------------------------- |
| MultipleInputs        | Handle multiple datasets with different formats |
| Reduce-Side Join      | Join datasets using employeeId                  |
| Hadoop Shuffle & Sort | Group related records automatically             |
| Single Reducer        | Ensure consistent maxPay calculation            |

---

# ⚠️ 8. Data Validation & Error Handling

## 🧑 Employee File Validation

The mapper skips:

* Empty lines
* Missing fields
* Invalid records
* Corrupted employee entries

---

## 💰 Payroll File Validation

The mapper skips:

* Malformed records
* Missing values
* Invalid salary values
* Non-numeric baseSalary or bonus fields

---

## ❗ Missing Employee Records

If payroll data references a non-existing employee:

```text
fullName = UNKNOWN EMPLOYEE
department = UNKNOWN
```

---

# 🔄 9. Combiner Decision

## ❌ Combiner Not Used

### Reason

Reducer logic contains:

* Join operations
* Data enrichment
* Global max salary calculations

These operations are:

* NOT associative
* NOT commutative

Therefore, using a Combiner may generate incorrect results.

---

# 🧩 10. Custom Writable Decision

## ❌ Custom Writable Not Required

### Reason

* Data structure is simple
* Hadoop `Text` type is sufficient
* No complex serialization is needed
* No composite keys are required

---

# 🚀 11. Compilation & Execution Guide

## 🟢 Step 1 — Open Shared Project Folder

```bash
cd /media/sf_EmployeePayroll
```

---

## 🟢 Step 2 — Verify Project Files

```bash
ls
```

---

## 🟢 Step 3 — Verify Input Data

```bash
ls Data
```

Expected:

```text
employees.csv
payroll.csv
```

---

## 🟢 Step 4 — Remove Old HDFS Input Directory

```bash
hdfs dfs -rm -r /user/cloudera/payroll/input
```

---

## 🟢 Step 5 — Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/cloudera/payroll/input
```

---

## 🟢 Step 6 — Upload Files to HDFS

```bash
hdfs dfs -put Data/employees.csv /user/cloudera/payroll/input/
hdfs dfs -put Data/payroll.csv /user/cloudera/payroll/input/
```

---

## 🟢 Step 7 — Verify Uploaded Files

```bash
hdfs dfs -ls /user/cloudera/payroll/input
```

---

## 🟢 Step 8 — Navigate to Source Code

```bash
cd Src
```

---

## 🟢 Step 9 — Compile Java Files

```bash
javac -classpath `hadoop classpath` -d . *.java
```

---

## 🟢 Step 10 — Create JAR File

```bash
jar -cvf payroll.jar *
```

---

## 🟢 Step 11 — Remove Old HDFS Output Directory

```bash
hdfs dfs -rm -r /user/cloudera/payroll/output
```

---

## 🟢 Step 12 — Run Hadoop MapReduce Job

```bash
hadoop jar payroll.jar com.payrolljoin.PayrollJoinDriver \
/user/cloudera/payroll/input/employees.csv \
/user/cloudera/payroll/input/payroll.csv \
/user/cloudera/payroll/output
```

---

## 🟢 Step 13 — View Sample Output

```bash
hdfs dfs -cat /user/cloudera/payroll/output/part-r-00000 | head -20 2>/dev/null
```

---

## 🟢 Step 14 — Download Output to Shared Folder

```bash
hdfs dfs -get /user/cloudera/payroll/output /media/sf_EmployeePayroll/payroll_output
```

The output will be available inside:

```text
F:\faculty\Level 3 S_2\Introduction to Big Data\Project\HadoopMapReduce\Employee Payroll Join using Hadoop MapReduce\payroll_output
```

---

# 📁 12. Output Structure

```text
payroll_output
│
├── part-r-00000
└── _SUCCESS
```

---

# 🧪 13. Testing Strategy

## ✔ Functional Testing

Validated:

* Join correctness
* totalPay calculation
* maxPay calculation
* Output formatting

---

## ✔ Edge Case Testing

Tested scenarios:

* Missing employee records
* Invalid salary values
* Empty lines
* Corrupted records
* Partial payroll records

---

## ✔ Scalability Testing

* Tested using large duplicated datasets
* Verified Hadoop processing consistency
* Confirmed successful processing of datasets larger than 1GB

---

# 📄 14. Assumptions

1. Input files are CSV formatted.
2. Input files do not contain header rows.
3. employeeId is unique in employee records.
4. Salary and bonus values are numeric.
5. One output row is generated for every payroll transaction.
6. Reducer count is fixed to 1.

---

# 📦 15. Deliverables

* ✅ Java Source Code
* ✅ README Documentation
* ✅ Sample Input Files
* ✅ Sample Output Files
* ✅ Assumptions File
* ✅ Fully Commented Code

---

# 💡 16. Key Design Highlights

* Clean Reduce-Side Join implementation
* Efficient processing of heterogeneous datasets
* Strong validation and error handling
* Modular and maintainable architecture
* Hadoop best practices applied
* Scalable distributed processing solution

---

# 👨‍💻 17. Author

Developed as part of a Big Data / Hadoop MapReduce academic project focused on:

* Distributed data processing
* Hadoop ecosystem concepts
* Large-scale data integration
* Scalable analytics systems
