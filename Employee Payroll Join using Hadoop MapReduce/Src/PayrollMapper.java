package com.payrolljoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * PayrollMapper reads payroll transaction records.
 *
 * Input format  : payrollId,employeeId,month,baseSalary,bonus
 * Output format : key = employeeId (Text)
 *                 value = "pay~month,baseSalary,bonus" (Text)
 *
 * The "pay~" prefix lets the Reducer distinguish payroll records from
 * employee records that arrive under the same key.
 *
 * Records with non-integer baseSalary or bonus are silently skipped so
 * that a single bad record cannot crash the job on a large dataset.
 */
public class PayrollMapper extends Mapper<LongWritable, Text, Text, Text> {

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
        if (line.toLowerCase().startsWith("payrollid")) {
            return;
        }

        String[] fields = line.split(",", -1);   // -1 keeps trailing empty tokens

        // Validate: must have exactly 5 fields
        if (fields.length != 5) {
            System.err.println("[PayrollMapper] Skipping malformed line (expected 5 fields): " + line);
            context.getCounter("PayrollMapper", "MalformedLines").increment(1);
            return;
        }

        // Fields: payrollId(0), employeeId(1), month(2), baseSalary(3), bonus(4)
        String employeeId  = fields[1].trim();
        String month       = fields[2].trim();
        String salaryStr   = fields[3].trim();
        String bonusStr    = fields[4].trim();

        // Skip records with empty mandatory fields
        if (employeeId.isEmpty() || month.isEmpty() || salaryStr.isEmpty() || bonusStr.isEmpty()) {
            System.err.println("[PayrollMapper] Skipping line with empty fields: " + line);
            context.getCounter("PayrollMapper", "EmptyFieldLines").increment(1);
            return;
        }

        // Parse baseSalary – skip non-integer values
        int baseSalary;
        try {
            baseSalary = Integer.parseInt(salaryStr);
        } catch (NumberFormatException e) {
            System.err.println("[PayrollMapper] Skipping line with non-integer baseSalary: " + line);
            context.getCounter("PayrollMapper", "NonIntegerSalary").increment(1);
            return;
        }

        // Parse bonus – skip non-integer values
        int bonus;
        try {
            bonus = Integer.parseInt(bonusStr);
        } catch (NumberFormatException e) {
            System.err.println("[PayrollMapper] Skipping line with non-integer bonus: " + line);
            context.getCounter("PayrollMapper", "NonIntegerBonus").increment(1);
            return;
        }

        // Emit: key = employeeId, value = "pay~month,baseSalary,bonus"
        outKey.set(employeeId);
        outValue.set("pay~" + month + "," + baseSalary + "," + bonus);
        context.write(outKey, outValue);
    }
}