package com.payrolljoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * EmployeeMapper reads employee lookup records.
 *
 * Input format  : employeeId,firstName,lastName,department
 * Output format : key = employeeId (Text)
 *                 value = "emp~firstName,lastName,department" (Text)
 *
 * The "emp~" prefix lets the Reducer distinguish employee records from
 * payroll records that arrive under the same key.
 */
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip blank lines
        if (line.isEmpty()) {
            return;
        }

        // Skip the CSV header row (case-insensitive check)
        if (line.toLowerCase().startsWith("employeeid")) {
            return;
        }

        String[] fields = line.split(",", -1);   // -1 keeps trailing empty tokens

        // Validate: must have exactly 4 fields
        if (fields.length != 4) {
            // Log the malformed line and skip it
            System.err.println("[EmployeeMapper] Skipping malformed line (expected 4 fields): " + line);
            context.getCounter("EmployeeMapper", "MalformedLines").increment(1);
            return;
        }

        String employeeId  = fields[0].trim();
        String firstName   = fields[1].trim();
        String lastName    = fields[2].trim();
        String department  = fields[3].trim();

        // Skip records with empty mandatory fields
        if (employeeId.isEmpty() || firstName.isEmpty() || lastName.isEmpty() || department.isEmpty()) {
            System.err.println("[EmployeeMapper] Skipping line with empty fields: " + line);
            context.getCounter("EmployeeMapper", "EmptyFieldLines").increment(1);
            return;
        }

        // Emit: key = employeeId, value = "emp~firstName,lastName,department"
        outKey.set(employeeId);
        outValue.set("emp~" + firstName + "," + lastName + "," + department);
        context.write(outKey, outValue);
    }
}