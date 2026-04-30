# Employee Payroll Join using Hadoop MapReduce

## 📌 Project Overview

This project implements a Hadoop MapReduce solution to join two datasets:

* Employee Directory (Lookup Data)
* Payroll Transactions (Large Dataset)

The system processes large-scale data (≥ 1GB) and produces enriched payroll records including total salary and maximum salary per employee.

---

## 📂 Input Data

### 1. Employees File

Format:

```
employeeId,firstName,lastName,department
```

Example:

```
EMP01,Nour,Hassan,Engineering
```

---

### 2. Payroll File

Format:

```
payrollId,employeeId,month,baseSalary,bonus
```

Example:

```
PR001,EMP01,Jan,8000,500
```

---

## 📤 Output Format

```
employeeId fullName,department,month,totalPay,maxPay
```

Example:

```
EMP01 Nour Hassan,Engineering,Jan,8500,8700
```

---

## ⚙️ MapReduce Design

### 🔹 EmployeeMapper

* Reads employee records
* Emits:

```
key = employeeId
value = emp~firstName,lastName,department
```

---

### 🔹 PayrollMapper

* Reads payroll records
* Validates numeric values
* Emits:

```
key = employeeId
value = pay~month,baseSalary,bonus
```

---

### 🔹 PayrollReducer

* Joins employee and payroll data
* Calculates:

  * totalPay = baseSalary + bonus
  * maxPay per employee
* Handles missing employees:

```
UNKNOWN EMPLOYEE,UNKNOWN
```

---

### 🔹 Driver

* Uses MultipleInputs
* Registers both mappers
* Uses one reducer
* Submits the job

---

## 📊 Large Data Handling

* Tested with dataset > 1GB
* Output size: ~1.2 GB
* Efficient streaming processing

---

## ⚠️ Data Validation

* Skips malformed records
* Handles:

  * missing fields
  * invalid salary values
  * unknown employee IDs

---

## 🚀 Execution Steps

### 🟢 1. Access Shared Folder

```bash
cd /media/sf_HadoopMapReduceProject
```

---

### 🟢 2. Enter Project Directory

```bash
cd "Employee Payroll Join using Hadoop MapReduce"
```

---

### 🟢 3. Verify Data

```bash
ls Data
```

---

### 🟢 4. Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/cloudera/payroll/input
```

---

### 🟢 5. Upload Data to HDFS

```bash
hdfs dfs -put Data/employees.csv /user/cloudera/payroll/input/
hdfs dfs -put Data/payroll.csv /user/cloudera/payroll/input/
```

---

### 🟢 6. Verify Upload

```bash
hdfs dfs -ls /user/cloudera/payroll/input
```

---

### 🟢 7. Navigate to Source Code

```bash
cd Src
```

---

### 🟢 8. Compile Java Code

```bash
javac -classpath `hadoop classpath` -d . *.java
```

---

### 🟢 9. Create JAR File

```bash
jar -cvf payroll.jar *
```

---

### 🟢 10. Remove Old Output

```bash
hdfs dfs -rm -r /user/cloudera/payroll/output
```

---

### 🟢 11. Run MapReduce Job

```bash
hadoop jar payroll.jar com.payrolljoin.PayrollJoinDriver \
/user/cloudera/payroll/input/employees.csv \
/user/cloudera/payroll/input/payroll.csv \
/user/cloudera/payroll/output
```

---

## 📊 Output Verification

### View Sample Output

```bash
hdfs dfs -cat /user/cloudera/payroll/output/part-r-00000 | head -20 2>/dev/null
```

---

### Check Output Size

```bash
hdfs dfs -du -h /user/cloudera/payroll/output
```

---

### Download Output to Local Machine

```bash
hdfs dfs -get /user/cloudera/payroll/output /media/sf_HadoopMapReduceProject/payroll_output
```

---

## 📁 Output Structure

```
payroll_output/
 ├── part-r-00000
 └── _SUCCESS
```

---

## 📌 Key Achievements

* Successfully processed dataset larger than 1GB
* Implemented Reduce-side Join using Hadoop
* Applied data validation and error handling
* Generated scalable and efficient output

---

## 🧠 Assumptions

* Employee file is smaller than payroll file
* Payroll may contain invalid or unmatched records
* Reducer handles missing employee cases

---

## 🔥 Conclusion

This project demonstrates the ability to:

* Process large-scale data using Hadoop
* Perform distributed joins efficiently
* Handle real-world data inconsistencies

---

## 👨‍💻 Author

Big Data Project – Hadoop MapReduce Implementation
