package com.payrolljoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PayrollReducer performs a reduce-side join between employee and payroll data.
 *
 * For each employeeId it receives a mix of:
 *   - "emp~firstName,lastName,department"  (from EmployeeMapper)
 *   - "pay~month,baseSalary,bonus"         (from PayrollMapper)
 *
 * Join logic:
 *   1. Collect the single employee record (if present).
 *   2. Collect all payroll records.
 *   3. After reading all values:
 *        - Compute totalPay = baseSalary + bonus for each payroll record.
 *        - Track maxPay across all payroll records for this employee.
 *        - Emit one output line per payroll record.
 *
 * Output format: employeeId fullName,department,month,totalPay,maxPay
 *
 * If no employee record exists for a given employeeId the reducer uses
 * "UNKNOWN EMPLOYEE" and "UNKNOWN" as placeholders, ensuring payroll
 * records for unknown employees still appear in the output.
 *
 * NOTE: A Combiner is intentionally NOT used because this is a
 * reduce-side join.  The aggregation (maxPay) is only meaningful after
 * ALL payroll records for an employee have been gathered, which only
 * happens at the Reducer.  Applying any partial aggregation at the
 * Mapper side (via a Combiner) would produce incorrect maxPay values.
 */
public class PayrollReducer extends Reducer<Text, Text, Text, Text> {

    // Prefix constants match those used in the Mappers
    private static final String EMP_PREFIX = "emp~";
    private static final String PAY_PREFIX = "pay~";

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // ── Step 1: Separate employee record from payroll records ──────────
        String employeeInfo = null;          // "firstName,lastName,department"
        List<String> payrollRecords = new ArrayList<>();  // each = "month,baseSalary,bonus"

        for (Text val : values) {
            String v = val.toString();

            if (v.startsWith(EMP_PREFIX)) {
                // Store the employee info (there should be only one per employeeId)
                employeeInfo = v.substring(EMP_PREFIX.length());
            } else if (v.startsWith(PAY_PREFIX)) {
                // Collect payroll records; copy the string because Hadoop reuses the
                // Text object across iterations
                payrollRecords.add(v.substring(PAY_PREFIX.length()));
            } else {
                // Unknown prefix – log and skip
                System.err.println("[PayrollReducer] Unknown value prefix for key "
                        + key.toString() + ": " + v);
                context.getCounter("PayrollReducer", "UnknownValuePrefix").increment(1);
            }
        }

        // ── Step 2: Resolve employee name and department ───────────────────
        String fullName;
        String department;

        if (employeeInfo != null) {
            // employeeInfo = "firstName,lastName,department"
            String[] empFields = employeeInfo.split(",", -1);
            if (empFields.length == 3) {
                fullName   = empFields[0].trim() + " " + empFields[1].trim();
                department = empFields[2].trim();
            } else {
                // Malformed employee record
                System.err.println("[PayrollReducer] Malformed employee info for key "
                        + key.toString() + ": " + employeeInfo);
                fullName   = "UNKNOWN EMPLOYEE";
                department = "UNKNOWN";
            }
        } else {
            // No matching employee record – use placeholder values
            fullName   = "UNKNOWN EMPLOYEE";
            department = "UNKNOWN";
            context.getCounter("PayrollReducer", "UnmatchedEmployees").increment(1);
        }

        // ── Step 3: Skip if there are no payroll records ──────────────────
        if (payrollRecords.isEmpty()) {
            // Employee exists but has no payroll records – nothing to emit
            context.getCounter("PayrollReducer", "EmployeesWithNoPayroll").increment(1);
            return;
        }

        // ── Step 4: First pass – compute all totalPay values and find maxPay ─
        List<int[]> parsed = new ArrayList<>(payrollRecords.size());
        int maxPay = Integer.MIN_VALUE;
        String[] monthLabels = new String[payrollRecords.size()];

        for (int i = 0; i < payrollRecords.size(); i++) {
            String record = payrollRecords.get(i);
            // record = "month,baseSalary,bonus"
            String[] parts = record.split(",", -1);

            if (parts.length != 3) {
                System.err.println("[PayrollReducer] Malformed payroll record for key "
                        + key.toString() + ": " + record);
                context.getCounter("PayrollReducer", "MalformedPayrollRecords").increment(1);
                parsed.add(null);
                continue;
            }

            try {
                int baseSalary = Integer.parseInt(parts[1].trim());
                int bonus      = Integer.parseInt(parts[2].trim());
                int totalPay   = baseSalary + bonus;

                monthLabels[i] = parts[0].trim();
                parsed.add(new int[]{baseSalary, bonus, totalPay});

                if (totalPay > maxPay) {
                    maxPay = totalPay;
                }
            } catch (NumberFormatException e) {
                System.err.println("[PayrollReducer] Non-integer salary/bonus in record for key "
                        + key.toString() + ": " + record);
                context.getCounter("PayrollReducer", "NonIntegerPayrollValues").increment(1);
                parsed.add(null);
            }
        }

        // If all payroll records were malformed, nothing to emit
        if (maxPay == Integer.MIN_VALUE) {
            return;
        }

        // ── Step 5: Second pass – emit one output line per valid payroll record ─
        // Output format: employeeId fullName,department,month,totalPay,maxPay
        String employeeId = key.toString();

        for (int i = 0; i < parsed.size(); i++) {
            if (parsed.get(i) == null) {
                continue;   // skip malformed records
            }

            int totalPay = parsed.get(i)[2];
            String month = monthLabels[i];

            outKey.set(employeeId + " " + fullName);
            outValue.set(department + "," + month + "," + totalPay + "," + maxPay);
            context.write(outKey, outValue);
        }
    }
}