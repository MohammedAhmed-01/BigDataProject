# 🧾 Age Group Partitioning using Hadoop MapReduce

---

# 📌 1. Project Overview

This project analyzes large-scale demographic data using Hadoop MapReduce.

The main objective is to classify people into age groups and calculate demographic statistics, mainly:

- Average income for each age group

The project uses:

- Hadoop MapReduce
- Custom Partitioner
- Combiner Optimization
- Multiple Reducers
- Large-scale datasets (>1GB)

The system is designed to be:

- ✅ Scalable
- ✅ Fault tolerant
- ✅ Efficient for big data processing
- ✅ Optimized using Combiner and Partitioner

---

# 🎯 2. Task Objective

The goal of this project is to:

1. Read demographic records
2. Extract age and income
3. Assign each person to an age group
4. Partition data by age group
5. Calculate average income for each group
6. Ensure each reducer processes one age group only

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
18-24   Avg Income: 2498
25-34   Avg Income: 5240
35-44   Avg Income: 7745
45-54   Avg Income: 9498
55+     Avg Income: 7480
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
│   └── AgeGroupDriver.java
│
├── output_agegroup
│   ├── _SUCCESS
│   ├── part-r-00000
│   ├── part-r-00001
│   ├── part-r-00002
│   ├── part-r-00003
│   └── part-r-00004
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

- Reads each demographic record
- Extracts:
  - age
  - income
- Validates the data
- Assigns each person to an age group

### 📤 Mapper Output

```text
age_group, income
```

### ✅ Example

```text
25-34   3000
```

---

## 🔹 7.2 Custom Partitioner

The custom Hadoop Partitioner ensures that:

- Each reducer receives one specific age group only

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

## 🔹 7.3 Combiner

The Combiner improves performance by reducing shuffle traffic.

Instead of sending all income records directly to reducers:

- Partial sums
- Partial counts

are calculated locally before shuffle.

### Benefits

- Reduces network traffic
- Improves execution speed
- Optimizes large-scale processing

---

## 🔹 7.4 Reducer

The Reducer receives all records for one age group.

It calculates:

```text
Average Income = Total Income / Number of Records
```

The reducer emits one final output line per age group.

---

## 🔹 7.5 Driver

The Driver configures the Hadoop job.

It sets:

- Mapper class
- Reducer class
- Combiner class
- Custom Partitioner
- Input path
- Output path
- Number of reducers = 5

---

# 🧠 8. Data Validation

The project validates input records using Hadoop Counters.

Validation checks include:

- Malformed lines
- Missing fields
- Invalid age values
- Out-of-range age values
- Invalid income values
- Non-numeric values

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

## 🟢 Step 2 — Verify Project Files

```bash
ls
```

Expected:

```text
Data
Documents
output_agegroup
screenshots
Scripts
src
Readme.md
```

---

## 🟢 Step 3 — Verify Java Source Files

```bash
ls src
```

Expected:

```text
AgeGroupMapper.java
AgeGroupReducer.java
AgeGroupCombiner.java
AgeGroupPartitioner.java
AgeGroupDriver.java
```

---

# 🔨 11. Compilation Steps

---

## 🟢 Step 4 — Compile Java Files

```bash
javac -classpath `hadoop classpath` -d . src/*.java
```

---

## 🟢 Step 5 — Verify Compiled Classes

```bash
ls *.class
```

Expected:

```text
AgeGroupCombiner.class
AgeGroupDriver.class
AgeGroupMapper.class
AgeGroupPartitioner.class
AgeGroupReducer.class
```

---

## 🟢 Step 6 — Create JAR File

```bash
jar -cvf agegroup.jar *.class
```

---

# 🗂️ 12. HDFS Setup

---

## 🟢 Step 7 — Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/cloudera/agegroup/input
```

---

## 🟢 Step 8 — Remove Old Input File

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
```

If you see:

```text
No such file or directory
```

that is normal for the first run.

---

## 🟢 Step 9 — Upload Dataset to HDFS

```bash
hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

---

## 🟢 Step 10 — Verify Uploaded Dataset

```bash
hdfs dfs -ls /user/cloudera/agegroup/input
```

Expected:

```text
demographic_data.csv
```

---

# ▶️ 13. Run Hadoop Job

---

## 🟢 Step 11 — Remove Old Output Folder

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

---

## 🟢 Step 12 — Execute MapReduce Job

```bash
hadoop jar agegroup.jar AgeGroupDriver \
/user/cloudera/agegroup/input \
/user/cloudera/agegroup/output
```

---

# 📤 14. Verify Output

---

## 🟢 Step 13 — Check Output Files

```bash
hdfs dfs -ls /user/cloudera/agegroup/output
```

Expected:

```text
_SUCCESS
part-r-00000
part-r-00001
part-r-00002
part-r-00003
part-r-00004
```

There are five output files because the job uses five reducers.

---

## 🟢 Step 14 — Display Final Output

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*
```

Expected:

```text
18-24   Avg Income: XXXX
25-34   Avg Income: XXXX
35-44   Avg Income: XXXX
45-54   Avg Income: XXXX
55+     Avg Income: XXXX
```

---

# 💾 15. Download Output Locally

---

## 🟢 Step 15 — Download Output Folder

```bash
hdfs dfs -get /user/cloudera/agegroup/output output_agegroup
```

---

## 🟢 Step 16 — Merge Reducer Outputs

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-* > final_output.txt
```

---

## 🟢 Step 17 — Verify Final Output File

```bash
cat final_output.txt
```

---

# 🛠️ 16. Useful HDFS Commands

---

## List HDFS Directory

```bash
hdfs dfs -ls /user/cloudera/agegroup
```

---

## List Input Folder

```bash
hdfs dfs -ls /user/cloudera/agegroup/input
```

---

## List Output Folder

```bash
hdfs dfs -ls /user/cloudera/agegroup/output
```

---

## View Output

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*
```

---

## Remove Input File

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
```

---

## Remove Output Folder

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

---

# ⚠️ 17. Common Errors and Solutions

---

## ❌ Error 1 — Output Directory Already Exists

### Error

```text
FileAlreadyExistsException: Output directory already exists
```

### Solution

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

---

## ❌ Error 2 — File Already Exists

### Error

```text
put: File exists
```

### Solution

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv

hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

---

## ❌ Error 3 — Permission Denied

### Solution

```bash
sudo usermod -aG vboxsf cloudera

reboot
```

---

## ❌ Error 4 — Class Name Mismatch

### Example Error

```text
class AgeGroupDriver is public, should be declared in a file named AgeGroupDriver.java
```

### Solution

Ensure file names exactly match class names.

---

## ❌ Error 5 — Class Not Found

### Solution

Recompile and recreate the JAR:

```bash
javac -classpath `hadoop classpath` -d . src/*.java

jar -cvf agegroup.jar *.class
```

---

# ⚡ 18. Final Run Commands Summary

```bash
cd /media/sf_AgeGroupPartitioning

javac -classpath `hadoop classpath` -d . src/*.java

jar -cvf agegroup.jar *.class

hdfs dfs -mkdir -p /user/cloudera/agegroup/input

hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv

hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/

hdfs dfs -rm -r /user/cloudera/agegroup/output

hadoop jar agegroup.jar AgeGroupDriver \
/user/cloudera/agegroup/input \
/user/cloudera/agegroup/output

hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*

hdfs dfs -get /user/cloudera/agegroup/output output_agegroup

hdfs dfs -cat /user/cloudera/agegroup/output/part-r-* > final_output.txt
```

---

# 📌 19. Notes

- The project uses 5 reducers
- Each reducer handles one age group only
- The final output contains 5 summary records
- The dataset size exceeds 1GB
- Hadoop Counters track valid and invalid records
- The Combiner reduces shuffle traffic
- The custom Partitioner satisfies task requirements

---

# ✅ 20. Conclusion

This project successfully implements Age Group Partitioning using Hadoop MapReduce.

The system processes large-scale demographic datasets, validates records, partitions data using a custom Hadoop Partitioner, and calculates average income statistics efficiently using distributed processing.