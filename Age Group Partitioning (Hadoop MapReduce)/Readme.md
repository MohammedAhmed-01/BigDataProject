# Age Group Partitioning using Hadoop MapReduce

## Project Overview

This project analyzes large-scale demographic data using Hadoop MapReduce.

The main goal is to group people by age range and calculate demographic statistics, mainly the average income for each age group.

A custom Hadoop Partitioner is used to make sure that each reducer processes only one specific age group.

---

## Task 15: Age Group Partitioning

### Input Format

```csv
person_id,age,income,employment_status,education_level
P001,25,3000,Employed,Bachelor
P002,35,5000,Employed,Master
P003,45,6000,Employed,Bachelor
P004,22,2500,Student,High School
P005,55,7000,Employed,PhD
```

---

## Output Format

The final output contains one line for each age group.

Example:

```text
18-24   Avg Income: 2498
25-34   Avg Income: 5240
35-44   Avg Income: 7745
45-54   Avg Income: 9498
55+     Avg Income: 7480
```

---

## Age Groups

The project uses five age groups:

```text
18-24
25-34
35-44
45-54
55+
```

---

## Project Structure

```text
Age Group Partitioning (Hadoop MapReduce)/
│
├── Data/
│   └── demographic_data.csv
│
├── Scripts/
│   └── generate_demographic_data.py
│
├── src/
│   ├── AgeGroupMapper.java
│   ├── AgeGroupReducer.java
│   ├── AgeGroupCombiner.java
│   ├── AgeGroupPartitioner.java
│   └── AgeGroupDriver.java
│
├── output_agegroup/
│   ├── _SUCCESS
│   ├── part-r-00000
│   ├── part-r-00001
│   ├── part-r-00002
│   ├── part-r-00003
│   └── part-r-00004
│
├── agegroup.jar
└── Readme.md
```

---

## Components Explanation

### 1. Mapper

The Mapper reads each input record and extracts:

- Age
- Income

It validates the input data and assigns each person to an age group.

Mapper output:

```text
age_group, income
```

Example:

```text
25-34   3000
```

---

### 2. Custom Partitioner

The custom partitioner sends each age group to a specific reducer.

Example:

```text
18-24  -> Reducer 0
25-34  -> Reducer 1
35-44  -> Reducer 2
45-54  -> Reducer 3
55+    -> Reducer 4
```

This satisfies the requirement that each reducer processes one age group only.

---

### 3. Combiner

The Combiner is used to reduce the amount of data transferred between the Mapper and Reducer.

Instead of sending all income records directly to reducers, the Combiner calculates partial sums and counts locally.

This improves performance, especially when processing large datasets.

---

### 4. Reducer

The Reducer receives all values for one age group and calculates:

```text
Average Income = Total Income / Number of Records
```

The final output contains one result per age group.

---

### 5. Driver

The Driver configures the MapReduce job.

It sets:

- Mapper class
- Reducer class
- Combiner class
- Custom Partitioner class
- Input path
- Output path
- Number of reducers = 5

---

## Data Validation

The project handles invalid records using Hadoop Counters.

Validation includes:

- Malformed lines
- Invalid age values
- Out-of-range age values
- Invalid income values
- Missing or unparseable numeric values

Example counters after running the job:

```text
DataQuality
    InvalidAge
    InvalidIncome
    MalformedLines
    OutOfRangeAge
    ValidRecords
```

---

## Large File Handling

The input dataset is generated to be larger than 1GB.

Example from Hadoop job counters:

```text
HDFS: Number of bytes read = 1102330521
```

This confirms that the project successfully processes large-scale data.

---

# Full Commands from Start to End

## 1. Open Cloudera Terminal

Start the Cloudera QuickStart VM and open the terminal.

---

## 2. Go to the Shared Project Folder

```bash
cd "/media/sf_Age_Group_Partitioning_(Hadoop_MapReduce)"
```

Check the files:

```bash
ls
```

Expected output:

```text
Data  Documents  Readme.md  screenshots  Scripts  src
```

---

## 3. Check Java Source Files

```bash
ls src
```

Expected files:

```text
AgeGroupMapper.java
AgeGroupReducer.java
AgeGroupCombiner.java
AgeGroupPartitioner.java
AgeGroupDriver.java
```

---

## 4. Compile Java Files

```bash
javac -classpath `hadoop classpath` -d . src/*.java
```

If there is no error, the `.class` files will be created.

---

## 5. Create JAR File

```bash
jar -cvf agegroup.jar *.class
```

Expected output:

```text
added manifest
adding: AgeGroupCombiner.class
adding: AgeGroupDriver.class
adding: AgeGroupMapper.class
adding: AgeGroupPartitioner.class
adding: AgeGroupReducer.class
```

---

## 6. Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/cloudera/agegroup/input
```

---

## 7. Upload Dataset to HDFS

```bash
hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

If the file already exists, run:

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

---

## 8. Check Uploaded Dataset

```bash
hdfs dfs -ls /user/cloudera/agegroup/input
```

---

## 9. Remove Old Output Folder

Hadoop will not run if the output folder already exists.

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

If you see this message:

```text
No such file or directory
```

That is normal if this is the first run.

---

## 10. Run the MapReduce Job

```bash
hadoop jar agegroup.jar AgeGroupDriver /user/cloudera/agegroup/input /user/cloudera/agegroup/output
```

---

## 11. Check Output Files

```bash
hdfs dfs -ls /user/cloudera/agegroup/output
```

Expected files:

```text
_SUCCESS
part-r-00000
part-r-00001
part-r-00002
part-r-00003
part-r-00004
```

There are five output part files because the job uses five reducers.

---

## 12. Display Final Output

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*
```

Expected result:

```text
18-24   Avg Income: XXXX
25-34   Avg Income: XXXX
35-44   Avg Income: XXXX
45-54   Avg Income: XXXX
55+     Avg Income: XXXX
```

The output should contain only five lines because the final result is a summary for five age groups.

---

## 13. Download Output Folder Locally

```bash
hdfs dfs -get /user/cloudera/agegroup/output output_agegroup
```

This downloads the full output folder from HDFS to the local project directory.

---

## 14. Merge All Reducer Outputs into One File

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-* > final_output.txt
```

This creates a single local output file called:

```text
final_output.txt
```

---

# Optional: Generate the Dataset

If the dataset does not exist, run the Python script:

```bash
python Scripts/generate_demographic_data.py
```

Or, if your script accepts arguments:

```bash
python Scripts/generate_demographic_data.py --output Data/demographic_data.csv --target-size-gb 1.1
```

Check the generated file:

```bash
ls -lh Data/demographic_data.csv
```

---

# Useful HDFS Commands

## List HDFS Directory

```bash
hdfs dfs -ls /user/cloudera/agegroup
```

## List Input Folder

```bash
hdfs dfs -ls /user/cloudera/agegroup/input
```

## List Output Folder

```bash
hdfs dfs -ls /user/cloudera/agegroup/output
```

## View Output

```bash
hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*
```

## Remove Input File

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
```

## Remove Output Folder

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

---

# Common Errors and Solutions

## Error 1: Output Directory Already Exists

Error:

```text
FileAlreadyExistsException: Output directory already exists
```

Solution:

```bash
hdfs dfs -rm -r /user/cloudera/agegroup/output
```

---

## Error 2: File Already Exists in Input

Error:

```text
put: File exists
```

Solution:

```bash
hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv
hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/
```

---

## Error 3: Permission Denied for Shared Folder

Solution:

```bash
sudo usermod -aG vboxsf cloudera
reboot
```

---

## Error 4: Class Name Does Not Match File Name

Example error:

```text
class AgeGroupDriver is public, should be declared in a file named AgeGroupDriver.java
```

Solution:

Make sure file names match class names exactly:

```text
AgeGroupDriver.java
AgeGroupMapper.java
AgeGroupReducer.java
AgeGroupCombiner.java
AgeGroupPartitioner.java
```

Java is case-sensitive.

---

## Error 5: Class Not Found

Make sure the JAR was created after successful compilation:

```bash
javac -classpath `hadoop classpath` -d . src/*.java
jar -cvf agegroup.jar *.class
```

Then run:

```bash
hadoop jar agegroup.jar AgeGroupDriver /user/cloudera/agegroup/input /user/cloudera/agegroup/output
```

---

# Final Run Commands Summary

Use this section if you want the short version only.

```bash
cd "/media/sf_Age_Group_Partitioning_(Hadoop_MapReduce)"

javac -classpath `hadoop classpath` -d . src/*.java

jar -cvf agegroup.jar *.class

hdfs dfs -mkdir -p /user/cloudera/agegroup/input

hdfs dfs -rm /user/cloudera/agegroup/input/demographic_data.csv

hdfs dfs -put Data/demographic_data.csv /user/cloudera/agegroup/input/

hdfs dfs -rm -r /user/cloudera/agegroup/output

hadoop jar agegroup.jar AgeGroupDriver /user/cloudera/agegroup/input /user/cloudera/agegroup/output

hdfs dfs -cat /user/cloudera/agegroup/output/part-r-*

hdfs dfs -get /user/cloudera/agegroup/output output_agegroup

hdfs dfs -cat /user/cloudera/agegroup/output/part-r-* > final_output.txt
```

---

# Notes

- The project uses 5 reducers.
- Each reducer handles one age group only.
- The final output contains 5 records.
- The input file is larger than 1GB.
- Hadoop counters prove that the job processed valid and invalid records.
- The Combiner reduces shuffle size and improves performance.
- The custom Partitioner satisfies the main requirement of the task.

---

# Conclusion

This project successfully implements Age Group Partitioning using Hadoop MapReduce.

It processes a large demographic dataset, validates input records, partitions records by age group, and calculates average income for each group using multiple reducers.