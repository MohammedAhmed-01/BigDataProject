# 🧾 Employee Payroll Join using Hadoop MapReduce

---

## 📌 1. Project Overview

This project implements a **Reduce-Side Join** using **Hadoop MapReduce (Java)** to integrate and process two heterogeneous datasets:

* **Employee Directory Dataset**
* **Monthly Payroll Transactions Dataset**

The system enriches payroll records with employee information and computes key financial metrics such as:

* Total Pay per record
* Maximum Pay per employee across all months

The solution is designed to be:

* ✅ Scalable (handles large datasets ≥ 1GB)
* ✅ Robust (handles malformed and missing data)
* ✅ Modular (clean separation of Mapper, Reducer, Driver)

---

## 🎯 2. Problem Statement

Given two datasets:

### Dataset 1: Employee Directory

Contains static employee information.

### Dataset 2: Payroll Transactions

Contains monthly salary and bonus data.

---

### 🧩 Objective

For each payroll record:

1. Join with employee data using `employeeId`
2. Compute:

   * `totalPay = baseSalary + bonus`
3. Track:

   * `maxPay` across all months for the same employee
4. Produce enriched output

---

## 📂 3. Input Data Specification

### 🧑 3.1 Employee File

**Schema:**

```text
employeeId, firstName, lastName, department
```

**Example:**

```text
EMP01,Nour,Hassan,Engineering
EMP02,Sara,Ali,HR
```

---

### 💰 3.2 Payroll File

**Schema:**

```text
payrollId, employeeId, month, baseSalary, bonus
```

**Example:**

```text
PR001,EMP01,Jan,8000,500
PR002,EMP01,Feb,8200,500
```

---

## 📤 4. Output Specification

**Schema:**

```text
employeeId    fullName,department,month,totalPay,maxPay
```

---

### ✅ Example Output

```text
EMP01    Nour Hassan,Engineering,Jan,8500,8700
EMP01    Nour Hassan,Engineering,Feb,8700,8700
EMP02    Sara Ali,HR,Jan,7300,7300
EMP04    UNKNOWN EMPLOYEE,UNKNOWN,Jan,6200,6200
```

---

## 🏗️ 5. Project Structure

```text
EmployeePayrollJoin/
│
├── src/
│   └── main/java/task1/
│       ├── EmployeeMapper.java
│       ├── PayrollMapper.java
│       ├── PayrollReducer.java
│       └── PayrollJoinDriver.java
│
├── input/
│   ├── employees.csv
│   └── payroll.csv
│
├── output_sample/
│   └── part-r-00000
│
├── README.md
├── assumptions.txt
└── sample_expected_output.txt
```

---

## ⚙️ 6. System Design & Architecture

This solution follows the **Reduce-Side Join Pattern**.

---

### 🔹 6.1 Data Flow

1. Input files are read independently
2. Each dataset is processed using a dedicated Mapper
3. Intermediate data is tagged and grouped by key (`employeeId`)
4. Reducer performs join + aggregation

---

### 🔹 6.2 Map Phase

#### 🧑 EmployeeMapper

* Reads employee records
* Extracts employee attributes
* Emits:

```text
(employeeId, "emp~firstName,lastName,department")
```

---

#### 💰 PayrollMapper

* Reads payroll records
* Validates numeric fields
* Emits:

```text
(employeeId, "pay~month,baseSalary,bonus")
```

---

### 🔹 6.3 Shuffle & Sort Phase

* Hadoop automatically groups all values by `employeeId`
* Ensures all related records go to the same reducer

---

### 🔹 6.4 Reduce Phase (PayrollReducer)

Reducer performs the following:

### Step 1: Data Separation

* Identify employee record (`emp~`)
* Collect all payroll records (`pay~`)

---

### Step 2: Data Enrichment

* Construct:

  * `fullName = firstName + lastName`
  * `department`

---

### Step 3: Aggregation

* For each payroll record:

  * Compute:

    ```text
    totalPay = baseSalary + bonus
    ```
* Track:

  ```text
  maxPay = max(totalPay across all months)
  ```

---

### Step 4: Output Generation

* Emit one output record per payroll entry

---

## 🧠 7. Hadoop Features Used

| Feature          | Purpose                              |
| ---------------- | ------------------------------------ |
| MultipleInputs   | Handle different input formats       |
| Reduce-Side Join | Combine datasets by key              |
| Single Reducer   | Ensure consistent maxPay calculation |

---

## ⚠️ 8. Data Validation & Error Handling

### 🧑 Employee File Validation

* Skip:

  * Empty lines
  * Missing fields
  * Invalid format

---

### 💰 Payroll File Validation

* Skip:

  * Malformed records
  * Missing fields
  * Non-numeric `baseSalary` or `bonus`

---

### ❗ Missing Join Case

If employee record is missing:

```text
fullName = UNKNOWN EMPLOYEE
department = UNKNOWN
```

---

## 🔄 9. Combiner Decision

❌ **Combiner NOT used**

### Reason:

Reducer logic includes:

* Join operation
* Data enrichment
* Global max calculation

These operations:

* Are NOT associative
* Are NOT commutative

➡️ Therefore, using a Combiner may produce incorrect results.

---

## 🧩 10. Custom Writable Decision

❌ **Custom Writable NOT required**

### Reason:

* Data is simple (strings and numbers)
* Hadoop `Text` type is sufficient
* No complex objects or multi-field keys needed

---

## 🚀 11. Compilation & Execution

### 🔧 Compile

```bash
javac -classpath `hadoop classpath` -d . *.java
```

### 📦 Create JAR

```bash
jar -cvf payrolljoin.jar task1/*.class
```

### ▶ Run Job

```bash
hadoop jar payrolljoin.jar task1.PayrollJoinDriver input/employees.csv input/payroll.csv output
```

---

## 🧪 12. Testing Strategy

### ✔ Functional Testing

* Validate join correctness
* Verify totalPay calculation
* Verify maxPay correctness

---

### ✔ Edge Case Testing

* Missing employee record
* Invalid numeric values
* Empty lines
* Partial/malformed records

---

### ✔ Scalability Testing

* Test using duplicated large datasets
* Ensure performance consistency

---

## 📄 13. Assumptions

1. Input files are CSV formatted
2. No header rows are included
3. `employeeId` is unique per employee
4. Salary and bonus are integers
5. Output is generated per payroll record
6. Reducer count is fixed to 1

---

## 📦 14. Deliverables

* ✅ Java Source Code
* ✅ README Documentation
* ✅ Sample Input Files
* ✅ Sample Output File
* ✅ Assumptions File
* ✅ Well-Commented Code

---

## 💡 15. Key Design Highlights

* Clean **Reduce-Side Join implementation**
* Efficient handling of heterogeneous datasets
* Strong **data validation strategy**
* Modular and maintainable code structure
* Fully aligned with **Hadoop best practices**

---

## 👨‍💻 16. Author

Developed as part of a **Big Data / Hadoop MapReduce academic project**, focusing on:

* Data integration
* Distributed processing
* Scalable analytics

---
