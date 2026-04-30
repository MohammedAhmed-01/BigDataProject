package com.payrolljoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PayrollJoinDriver configures and submits the Employee Payroll Join MapReduce job.
 *
 * Usage:
 *   hadoop jar employee-payroll-join-1.0.jar com.payrolljoin.PayrollJoinDriver \
 *       <employees-input-path> <payroll-input-path> <output-path>
 *
 * Design decisions:
 *   - MultipleInputs is used so that two different Mappers can read from
 *     two different input directories with two different record formats.
 *   - A single Reducer (setNumReduceTasks(1)) ensures that ALL records for
 *     a given employeeId arrive at the same Reducer instance, which is
 *     required for a correct reduce-side join.
 *   - No Combiner is registered because this is a reduce-side join and
 *     partial aggregation before the shuffle would produce wrong maxPay.
 */
public class PayrollJoinDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        // ── Argument validation ────────────────────────────────────────────
        if (args.length != 3) {
            System.err.println("Usage: PayrollJoinDriver <employees-input> <payroll-input> <output>");
            System.err.println("  employees-input : HDFS path containing employee CSV files");
            System.err.println("  payroll-input   : HDFS path containing payroll CSV files");
            System.err.println("  output          : HDFS path for job output (must not exist)");
            return 1;
        }

        Path employeesInputPath = new Path(args[0]);
        Path payrollInputPath   = new Path(args[1]);
        Path outputPath         = new Path(args[2]);

        // ── Job configuration ──────────────────────────────────────────────
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Employee Payroll Join");
        job.setJarByClass(PayrollJoinDriver.class);

        // ── MultipleInputs: register each mapper with its input path ───────
        // EmployeeMapper handles the employee lookup directory
        MultipleInputs.addInputPath(job, employeesInputPath,
                TextInputFormat.class, EmployeeMapper.class);

        // PayrollMapper handles the payroll transaction directory
        MultipleInputs.addInputPath(job, payrollInputPath,
                TextInputFormat.class, PayrollMapper.class);

        // ── Reducer configuration ──────────────────────────────────────────
        job.setReducerClass(PayrollReducer.class);

        // Single reducer guarantees all values for an employeeId are co-located
        job.setNumReduceTasks(1);

        // ── Output types ───────────────────────────────────────────────────
        // Both mappers emit (Text, Text); reducer emits (Text, Text)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ── Output path ────────────────────────────────────────────────────
        FileOutputFormat.setOutputPath(job, outputPath);

        // ── Submit and wait ────────────────────────────────────────────────
        boolean success = job.waitForCompletion(true /* verbose */);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PayrollJoinDriver(), args);
        System.exit(exitCode);
    }
}