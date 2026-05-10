# Employee Payroll Join using Hadoop MapReduce

## 📌 Project Overview

This project implements a Hadoop MapReduce solution to join two datasets:

- Employee Directory
- Payroll Transactions

The system processes large-scale payroll data and produces enriched payroll records containing employee information, monthly salary, total pay, and maximum pay per employee.

---

## 📂 Project Structure

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
└── payroll_output
```

---

## 📥 Input Files

### 1. Employees File

Path:

```bash
Data/employees.csv
```

Format:

```text
employeeId,firstName,lastName,department
```

Example:

```text
EMP01,Nour,Hassan,Engineering
```

---

### 2. Payroll File

Path:

```bash
Data/payroll.csv
```

Format:

```text
payrollId,employeeId,month,baseSalary,bonus
```

Example:

```text
PR001,EMP01,Jan,8000,500
```

---

## 📤 Output Format

```text
employeeId fullName,department,month,totalPay,maxPay
```

Example:

```text
EMP01 Nour Hassan,Engineering,Jan,8500,8700
```

---

## ⚙️ MapReduce Design

### EmployeeMapper

Reads employee data and emits:

```text
key = employeeId
value = emp~firstName,lastName,department
```

---

### PayrollMapper

Reads payroll data, validates salary values, and emits:

```text
key = employeeId
value = pay~month,baseSalary,bonus
```

---

### PayrollReducer

Joins employee records with payroll records and calculates:

* totalPay = baseSalary + bonus
* maxPay per employee

If an employee is missing from the employee file, it outputs:

```text
UNKNOWN EMPLOYEE,UNKNOWN
```

---

### Driver

The Driver class:

* Uses MultipleInputs
* Registers EmployeeMapper and PayrollMapper
* Uses PayrollReducer
* Runs the MapReduce job

---

# 🚀 Execution Guide

## 1️⃣ Open the Shared Project Folder

```bash
cd /media/sf_EmployeePayroll
```

Check project files:

```bash
ls
```

Check input data:

```bash
ls Data
```

You should see:

```text
employees.csv  payroll.csv
```

---

## 2️⃣ Remove Old HDFS Input Folder

```bash
hdfs dfs -rm -r /user/cloudera/payroll/input
```

---

## 3️⃣ Create New HDFS Input Folder

```bash
hdfs dfs -mkdir -p /user/cloudera/payroll/input
```

---

## 4️⃣ Upload Input Files to HDFS

```bash
hdfs dfs -put Data/employees.csv /user/cloudera/payroll/input/
hdfs dfs -put Data/payroll.csv /user/cloudera/payroll/input/
```

---

## 5️⃣ Verify Uploaded Files

```bash
hdfs dfs -ls /user/cloudera/payroll/input
```

Expected files:

```text
employees.csv
payroll.csv
```

---

## 6️⃣ Go to Source Code Folder

```bash
cd Src
```

---

## 7️⃣ Compile Java Files

```bash
javac -classpath `hadoop classpath` -d . *.java
```

This command compiles all Java files and creates the package folders.

---

## 8️⃣ Create JAR File

```bash
jar -cvf payroll.jar *
```

This creates:

```text
payroll.jar
```

---

## 9️⃣ Remove Old HDFS Output Folder

Hadoop will fail if the output folder already exists, so remove it first:

```bash
hdfs dfs -rm -r /user/cloudera/payroll/output
```

---

## 🔟 Run the MapReduce Job

```bash
hadoop jar payroll.jar com.payrolljoin.PayrollJoinDriver \
/user/cloudera/payroll/input/employees.csv \
/user/cloudera/payroll/input/payroll.csv \
/user/cloudera/payroll/output
```

---

## 1️⃣1️⃣ View Sample Output

```bash
hdfs dfs -cat /user/cloudera/payroll/output/part-r-00000 | head -20 2>/dev/null
```

---

## 1️⃣2️⃣ Download Output to Shared Folder

```bash
hdfs dfs -get /user/cloudera/payroll/output /media/sf_EmployeePayroll/payroll_output
```

After this step, the output will be available in:

```text
F:\faculty\Level 3 S_2\Introduction to Big Data\Project\HadoopMapReduce\Employee Payroll Join using Hadoop MapReduce\payroll_output
```

---

## 📁 Final Output Structure

```text
payroll_output
│
├── part-r-00000
└── _SUCCESS
```

---

## ✅ Full Commands

```bash
cd /media/sf_EmployeePayroll
ls
ls Data

hdfs dfs -rm -r /user/cloudera/payroll/input
hdfs dfs -mkdir -p /user/cloudera/payroll/input

hdfs dfs -put Data/employees.csv /user/cloudera/payroll/input/
hdfs dfs -put Data/payroll.csv /user/cloudera/payroll/input/

hdfs dfs -ls /user/cloudera/payroll/input

cd Src

javac -classpath `hadoop classpath` -d . *.java

jar -cvf payroll.jar *

hdfs dfs -rm -r /user/cloudera/payroll/output

hadoop jar payroll.jar com.payrolljoin.PayrollJoinDriver \
/user/cloudera/payroll/input/employees.csv \
/user/cloudera/payroll/input/payroll.csv \
/user/cloudera/payroll/output

hdfs dfs -cat /user/cloudera/payroll/output/part-r-00000 | head -20 2>/dev/null

hdfs dfs -get /user/cloudera/payroll/output /media/sf_EmployeePayroll/payroll_output
```

---

## ⚠️ Important Notes

* The Hadoop output directory must not exist before running the job.
* If `/user/cloudera/payroll/output` already exists, remove it first.
* The local output will be saved inside the shared folder so it can be accessed from Windows.
* The project uses Reduce-side Join.
* The payroll file is treated as the large dataset.
* The employee file is used as lookup/reference data.

---

## 📌 Key Achievements

* Processed large payroll data using Hadoop MapReduce
* Joined employee and payroll datasets
* Calculated total salary per payroll record
* Calculated maximum salary per employee
* Handled invalid and missing data
* Exported final output to Windows shared folder

---

## 👨‍💻 Author

Big Data Project – Hadoop MapReduce Implementation

```
```
