import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * AgeGroupDriver
 *
 * Configures and submits the Age-Group Partitioning MapReduce job.
 *
 * ─── Job overview ────────────────────────────────────────────────────
 *
 *   Input  : HDFS path containing demographic CSV files
 *   Output : HDFS path; one part-r-0000{N} file per age group
 *
 *   Mapper       : AgeGroupMapper      (emits Text key, IncomeCountWritable value)
 *   Combiner     : AgeGroupCombiner    (optional — merges IncomeCountWritables locally)
 *   Partitioner  : AgeGroupPartitioner (routes by age-group label)
 *   Reducer      : AgeGroupReducer     (produces final Text statistics line)
 *   Reducers     : 5                   (one per age group)
 *
 * ─── Key type decisions ───────────────────────────────────────────────
 *
 *   Map output key   : Text                  (age-group label)
 *   Map output value : IncomeCountWritable    (binary Writable — replaces "age,income" string)
 *   Job output key   : Text                  (age-group label — passed through by reducer)
 *   Job output value : Text                  (formatted statistics string)
 *
 * ─── Output file mapping ─────────────────────────────────────────────
 *
 *   part-r-00000  →  18-24
 *   part-r-00001  →  25-34
 *   part-r-00002  →  35-44
 *   part-r-00003  →  45-54
 *   part-r-00004  →  55+
 *
 * ─── Usage ───────────────────────────────────────────────────────────
 *
 *   # Compile all sources
 *   javac -classpath `hadoop classpath` -d . *.java
 *
 *   # Package
 *   jar -cvf agegroup.jar *.class
 *
 *   # Run WITH combiner (default)
 *   hadoop jar agegroup.jar AgeGroupDriver \
 *       -D agegroup.combiner=true           \
 *       /user/cloudera/input/demographics   \
 *       /user/cloudera/output/agegroup_with_combiner
 *
 *   # Run WITHOUT combiner
 *   hadoop jar agegroup.jar AgeGroupDriver \
 *       -D agegroup.combiner=false          \
 *       /user/cloudera/input/demographics   \
 *       /user/cloudera/output/agegroup_no_combiner
 */
public class AgeGroupDriver extends Configured implements Tool {

    /** Configuration property key to enable or disable the Combiner at runtime. */
    private static final String PROP_COMBINER = "agegroup.combiner";

    // ---------------------------------------------------------------
    // Entry point
    // ---------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AgeGroupDriver(), args);
        System.exit(exitCode);
    }

    // ---------------------------------------------------------------
    // Tool interface — called by ToolRunner after parsing -D flags
    // ---------------------------------------------------------------
    @Override
    public int run(String[] args) throws Exception {

        // ── Argument validation ──────────────────────────────────────
        if (args.length < 2) {
            System.err.println(
                    "Usage: AgeGroupDriver [-D agegroup.combiner=true|false] "
                    + "<inputPath> <outputPath>");
            return 1;
        }

        String inputPath  = args[0];
        String outputPath = args[1];

        // getConf() returns the Configuration already populated by
        // ToolRunner with any -D key=value pairs from the command line.
        Configuration conf = getConf();

        // ── Combiner toggle (default: enabled) ───────────────────────
        boolean useCombiner = conf.getBoolean(PROP_COMBINER, true);

        System.out.printf("[AgeGroupDriver] Input            : %s%n", inputPath);
        System.out.printf("[AgeGroupDriver] Output           : %s%n", outputPath);
        System.out.printf("[AgeGroupDriver] Combiner enabled : %b%n", useCombiner);

        // ── Delete existing output path if present ───────────────────
        Path output = new Path(outputPath);
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            System.out.printf(
                    "[AgeGroupDriver] Deleting existing output path: %s%n", outputPath);
            fs.delete(output, true);
        }

        // ── Build and configure the job ───────────────────────────────
        Job job = Job.getInstance(conf, "AgeGroup-Partitioning");
        job.setJarByClass(AgeGroupDriver.class);

        // ── Classes ───────────────────────────────────────────────────
        job.setMapperClass(AgeGroupMapper.class);
        job.setPartitionerClass(AgeGroupPartitioner.class);
        job.setReducerClass(AgeGroupReducer.class);

        // ── Optional Combiner ─────────────────────────────────────────
        // The Combiner's input/output types match the Mapper's output
        // types (Text key, IncomeCountWritable value), satisfying Hadoop's
        // combiner type-compatibility requirement.
        if (useCombiner) {
            job.setCombinerClass(AgeGroupCombiner.class);
            System.out.println("[AgeGroupDriver] Combiner: AgeGroupCombiner ENABLED");
        } else {
            System.out.println("[AgeGroupDriver] Combiner: DISABLED");
        }

        // ── Reducer count — must match AgeGroupPartitioner.NUM_PARTITIONS ──
        job.setNumReduceTasks(AgeGroupPartitioner.NUM_PARTITIONS);

        // ── Mapper output types ───────────────────────────────────────
        // The Mapper emits:
        //   key   = Text                 (age-group label)
        //   value = IncomeCountWritable  (per-record aggregate)
        //
        // These types are also consumed by the Combiner (if enabled) and
        // the Partitioner, so they must be set as the MAP output types,
        // not as the job output types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IncomeCountWritable.class);

        // ── Final reducer output types ────────────────────────────────
        // The Reducer emits:
        //   key   = Text  (age-group label — passed through)
        //   value = Text  (formatted statistics string)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ── Input / output formats ────────────────────────────────────
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // ── HDFS paths ────────────────────────────────────────────────
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output);

        // ── Submit and block until complete ───────────────────────────
        boolean success = job.waitForCompletion(true);

        // ── Print job counters ────────────────────────────────────────
        if (success) {
            System.out.println("\n=== Data Quality Counters ===");
            for (org.apache.hadoop.mapreduce.Counter c :
                    job.getCounters().getGroup("DataQuality")) {
                System.out.printf("  %-25s : %d%n", c.getName(), c.getValue());
            }
            System.out.println("\n=== Reducer Data Quality Counters ===");
            for (org.apache.hadoop.mapreduce.Counter c :
                    job.getCounters().getGroup("ReducerDataQuality")) {
                System.out.printf("  %-30s : %d%n", c.getName(), c.getValue());
            }
        }

        return success ? 0 : 1;
    }
}
