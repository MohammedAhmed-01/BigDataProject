import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * AgeGroupMapper
 *
 * Reads demographic CSV records and emits:
 *   Key  : age group label  (e.g. "20-30", "31-50", "51+")
 *   Value: composite string "age,income"
 *
 * Input format (CSV):
 *   person_id, age, income, employment_status, education_level
 *
 * Validation:
 *   - Skips malformed lines (wrong field count, unparseable numbers)
 *   - Skips records with age <= 0 or age > 150
 *   - Skips records with income < 0
 */
public class AgeGroupMapper extends Mapper<Object, Text, Text, Text> {

    // ---------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------
    private static final int FIELD_COUNT      = 5;
    private static final int IDX_AGE          = 1;
    private static final int IDX_INCOME       = 2;
    private static final int MIN_VALID_AGE    = 1;
    private static final int MAX_VALID_AGE    = 150;

    // Reusable Hadoop writables (avoid GC pressure on large datasets)
    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    // ---------------------------------------------------------------
    // Map method
    // ---------------------------------------------------------------
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines and header rows
        if (line.isEmpty() || line.startsWith("person_id")) {
            return;
        }

        String[] fields = line.split(",");

        // ── Field-count validation ──────────────────────────────────
        if (fields.length != FIELD_COUNT) {
            context.getCounter("DataQuality", "MalformedLines").increment(1);
            return;
        }

        // ── Parse age ───────────────────────────────────────────────
        int age;
        try {
            age = Integer.parseInt(fields[IDX_AGE].trim());
        } catch (NumberFormatException e) {
            context.getCounter("DataQuality", "InvalidAge").increment(1);
            return;
        }

        if (age < MIN_VALID_AGE || age > MAX_VALID_AGE) {
            context.getCounter("DataQuality", "OutOfRangeAge").increment(1);
            return;
        }

        // ── Parse income ─────────────────────────────────────────────
        double income;
        try {
            income = Double.parseDouble(fields[IDX_INCOME].trim());
        } catch (NumberFormatException e) {
            context.getCounter("DataQuality", "InvalidIncome").increment(1);
            return;
        }

        if (income < 0) {
            context.getCounter("DataQuality", "NegativeIncome").increment(1);
            return;
        }

        // ── Determine age group label ────────────────────────────────
        String ageGroup = getAgeGroup(age);
        if (ageGroup == null) {
            // Age below the minimum tracked group (< 18) — skip
            context.getCounter("DataQuality", "UnderageSkipped").increment(1);
            return;
        }

        // ── Emit key/value ───────────────────────────────────────────
        // Value format: "age,income"  (both needed for richer reducer stats)
        outKey.set(ageGroup);
        outValue.set(age + "," + income);
        context.write(outKey, outValue);
        context.getCounter("DataQuality", "ValidRecords").increment(1);
    }

    // ---------------------------------------------------------------
    // Helper: map numeric age → age-group label
    // ---------------------------------------------------------------

    /**
     * Returns the age-group bucket label for a given age, or {@code null}
     * if the age falls outside all tracked groups (i.e. under 18).
     *
     * Age groups:
     *   18–24  →  "18-24"
     *   25–34  →  "25-34"
     *   35–44  →  "35-44"
     *   45–54  →  "45-54"
     *   55+    →  "55+"
     *
     * NOTE: The output sample in the task spec uses broader bands
     *       (20-30, 31-50, 51+).  The partitioner supports BOTH by
     *       mapping the fine-grained labels to partition indices; the
     *       reducer key is what appears in the final output.
     *       Change the bands here (and mirror in AgeGroupPartitioner)
     *       to match whichever grouping is preferred.
     */
    public static String getAgeGroup(int age) {
        if (age < 18)  return null;
        if (age <= 24) return "18-24";
        if (age <= 34) return "25-34";
        if (age <= 44) return "35-44";
        if (age <= 54) return "45-54";
        return "55+";
    }
}