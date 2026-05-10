# 🧾 Age Group Partitioning using Hadoop MapReduce

---

# 📌 1. Project Overview

This project analyzes large-scale demographic data using Hadoop MapReduce.

The main objective is to classify people into age groups and calculate demographic statistics, mainly:

* Average income for each age group
* Minimum income
* Maximum income
* Average age
* Total number of people in each age group

The project uses:

* Hadoop MapReduce
* Custom Partitioner
* Combiner Optimization
* Custom Writable (IncomeCountWritable)
* Multiple Reducers
* Large-scale datasets (>1GB)

The system is designed to be:

* ✅ Scalable
* ✅ Fault tolerant
* ✅ Efficient for big data processing
* ✅ Optimized using Combiner and Partitioner

---

# 🎯 2. Task Objective

The goal of this project is to:

1. Read demographic records
2. Extract age and income
3. Assign each person to an age group
4. Partition data by age group
5. Calculate statistics for each group
6. Ensure each reducer processes one age group only
7. Compare execution with and without Combiner

---

# 📂 3. Input Dataset

## 📑 Input Format

```csv
person_id,age,income,employment_status,education_level
P001,25,3000,Employed,Bachelor
P002,35,5000,Employed,Master
P003,45,6000,Employed,Bachelor
P004,22,2500,Student,High School
P005,55,7000,Employed,PhD
```

---

# 📤 4. Output Format

The output contains one record for each age group.

## ✅ Example Output

```text
18-24   Avg Income: 2498 | Min: 1000 | Max: 4500 | Count: 500 | Avg Age: 22
25-34   Avg Income: 5240 | Min: 2500 | Max: 9000 | Count: 720 | Avg Age: 29
35-44   Avg Income: 7745 | Min: 3000 | Max: 12000 | Count: 680 | Avg Age: 39
45-54   Avg Income: 9498 | Min: 3500 | Max: 15000 | Count: 420 | Avg Age: 49
55+     Avg Income: 7480 | Min: 2000 | Max: 11000 | Count: 300 | Avg Age: 61
```

---

# 🧩 5. Age Groups

The project uses five age categories:

```text
18-24
25-34
35-44
45-54
55+
```

Each age group is processed by a dedicated reducer.

---

# 🏗️ 6. Project Structure

```text
Age Group Partitioning (Hadoop MapReduce)
│
├── Data
│   └── demographic_data.csv
│
├── Scripts
│   └── generate_demographic_data.py
│
├── src
│   ├── AgeGroupMapper.java
│   ├── AgeGroupReducer.java
│   ├── AgeGroupCombiner.java
│   ├── AgeGroupPartitioner.java
│   ├── AgeGroupDriver.java
│   └── IncomeCountWritable.java
│
├── output_with_combiner
├── output_without_combiner
│
├── screenshots
├── Documents
├── agegroup.jar
└── Readme.md
```

---

# ⚙️ 7. Hadoop Components Explanation

---

## 🔹 7.1 Mapper

The Mapper:

* Reads each demographic record
* Extracts:

  * age
  * income
* Validates the data
* Assigns each person to an age group
* Creates an IncomeCountWritable object

### 📤 Mapper Output

```text
key   = age_group
value = IncomeCountWritable
```

### ✅ Example

```text
25-34   (3000,1,25,3000,3000)
```

---

## 🔹 7.2 Custom Writable — IncomeCountWritable

The project uses a custom Hadoop Writable named:

```text
IncomeCountWritable
```

It stores:

* sumIncome
* count
* sumAge
* minIncome
* maxIncome

### Benefits

* Better Hadoop serialization
* Cleaner implementation
* Strongly typed values
* Faster than string parsing
* More scalable for large datasets

---

## 🔹 7.3 Custom Partitioner

The custom Hadoop Partitioner ensures that:

* Each reducer receives one specific age group only

### Example

```text
18-24  -> Reducer 0
25-34  -> Reducer 1
35-44  -> Reducer 2
45-54  -> Reducer 3
55+    -> Reducer 4
```

This satisfies the task requirement.

---

## 🔹 7.4 Combiner

The Combiner improves performance by reducing shuffle traffic.

Instead of sending all records directly to reducers:

* Partial sums
* Partial counts
* Partial min/max values

are calculated locally before shuffle.

### Benefits

* Reduces network traffic
* Improves execution speed
* Optimizes large-scale processing

---

## 🔹 7.5 Reducer

The Reducer receives all records for one age group.

It calculates:

```text
Average Income
Minimum Income
Maximum Income
Average Age
Count
```

The reducer emits one final output line per age group.

---

## 🔹 7.6 Driver

The Driver configures the Hadoop job.

It sets:

* Mapper class
* Reducer class
* Combiner class
* Custom Partitioner
* Input path
* Output path
* Number of reducers = 5
* IncomeCountWritable as Map Output Value

---

# 🧠 8. Data Validation

The project validates input records using Hadoop Counters.

Validation checks include:

* Malformed lines
* Missing fields
* Invalid age values
* Out-of-range age values
* Invalid income values
* Non-numeric values

---

## ✅ Example Hadoop Counters

```text
DataQuality
    InvalidAge
    InvalidIncome
    MalformedLines
    OutOfRangeAge
    ValidRecords
```

---

# 📊 9. Large File Handling

The project processes datasets larger than 1GB.

## Example Hadoop Counter

```text
HDFS: Number of bytes read = 1102330521
```

This confirms successful large-scale processing using Hadoop.

---

# 🚀 10. Full Execution Guide

---

## 🟢 Step 1 — Open Shared Folder

```bash
cd /media/sf_AgeGroupPartitioning
```

---

## 🟢 Step 2 — Remove Old Class Files

```bash
rm -rf *.class

find . -name "*.class" -delete
```

---

## 🟢 Step 3 — Compile Java Files

```bash
javac -classpath `hadoop classpath` -d . src/*.java
```

---

## 🟢 Step 4 — Create JAR File

```bash
jar -cvf agegroup.jar *.class
```

---

## 🟢 Step 5 — Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/cloudera/agegroup/input
```

---

## 🟢 Step 6 — Remove Old Input File

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
```

---

## 🟢 Step 7 — Upload Dataset to HDFS

```bash
hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

---

# ▶️ 11. Run WITH Combiner

---

## 🟢 Step 8 — Remove Old Output Folder

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output_with_combiner
```

---

## 🟢 Step 9 — Run Hadoop Job WITH Combiner

```bash
hadoop jar agegroup.jar AgeGroupDriver \
-D agegroup.combiner=true \
/user/cloudera/agegroup/input \
/user/cloudera/agegroup/output_with_combiner
```

---

## 🟢 Step 10 — Display Output WITH Combiner

```bash
hdfs dfs -cat /user/cloudera/agegroup/output_with_combiner/part-r-*
```

---

## 🟢 Step 11 — Download Output WITH Combiner

```bash
hdfs dfs -get /user/cloudera/agegroup/output_with_combiner output_with_combiner
```

---

## 🟢 Step 12 — Save Output WITH Combiner

```bash
hdfs dfs -cat /user/cloudera/agegroup/output_with_combiner/part-r-* > final_output_with_combiner.txt
```

---

# ▶️ 12. Run WITHOUT Combiner

---

## 🟢 Step 13 — Remove Old Output Folder

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output_without_combiner
```

---

## 🟢 Step 14 — Run Hadoop Job WITHOUT Combiner

```bash
hadoop jar agegroup.jar AgeGroupDriver \
-D agegroup.combiner=false \
/user/cloudera/agegroup/input \
/user/cloudera/agegroup/output_without_combiner
```

---

## 🟢 Step 15 — Display Output WITHOUT Combiner

```bash
hdfs dfs -cat /user/cloudera/agegroup/output_without_combiner/part-r-*
```

---

## 🟢 Step 16 — Download Output WITHOUT Combiner

```bash
hdfs dfs -get /user/cloudera/agegroup/output_without_combiner output_without_combiner
```

---

## 🟢 Step 17 — Save Output WITHOUT Combiner

```bash
hdfs dfs -cat /user/cloudera/agegroup/output_without_combiner/part-r-* > final_output_without_combiner.txt
```

---

# 📤 13. Expected Output Files

Because the project uses 5 reducers, Hadoop generates:

```text
part-r-00000
part-r-00001
part-r-00002
part-r-00003
part-r-00004
```

Each file represents one age group.

---

# ⚠️ 14. Common Errors and Solutions

---

## ❌ Error 1 — Output Directory Already Exists

### Error

```text
FileAlreadyExistsException
```

### Solution

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output_with_combiner
```

or

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output_without_combiner
```

---

## ❌ Error 2 — File Already Exists

### Solution

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
```

Then upload again.

---

## ❌ Error 3 — ClassCastException

### Cause

Old compiled .class files still exist.

### Solution

```bash
find . -name "*.class" -delete
```

Then recompile.

---

## ❌ Error 4 — Type Mismatch

### Cause

Some files still use:

```java
Text
```

instead of:

```java
IncomeCountWritable
```

### Solution

Ensure all mapper/combiner/reducer generic types match.

---

# 📈 15. Combiner Comparison

## WITH Combiner

* Less shuffle traffic
* Faster execution
* Better scalability
* Lower network overhead

## WITHOUT Combiner

* More shuffle traffic
* Slower execution
* More network usage

The final output remains identical in both cases.

---

# 📌 16. Notes

* The project uses 5 reducers
* Each reducer handles one age group only
* Hadoop Counters track valid and invalid records
* IncomeCountWritable improves serialization
* The Combiner reduces shuffle traffic
* The custom Partitioner satisfies task requirements
* The project supports large-scale demographic processing

---

# ✅ 17. Conclusion

This project successfully implements Age Group Partitioning using Hadoop MapReduce.

The system processes large-scale demographic datasets, validates records, partitions data using a custom Hadoop Partitioner, optimizes execution using Combiner and IncomeCountWritable, and calculates demographic statistics efficiently using distributed processing.
