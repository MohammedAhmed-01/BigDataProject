import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * AgeGroupMapper
 *
 * Reads demographic CSV records and emits:
 *   Key  : age-group label         (Text)       e.g. "25-34"
 *   Value: per-record aggregate     (IncomeCountWritable)
 *          → sumIncome = income
 *          → count     = 1
 *          → sumAge    = age
 *          → minIncome = income
 *          → maxIncome = income
 *
 * Using {@link IncomeCountWritable} as the value type eliminates the
 * brittle "age,income" and "C:…" string encoding previously used to
 * communicate between the Mapper, Combiner, and Reducer.  Hadoop's
 * binary serialisation is faster, more compact, and type-safe.
 *
 * Input format (CSV — 5 fields):
 *   person_id, age, income, employment_status, education_level
 *
 * Validation rules (unchanged from original):
 *   - Skips empty lines and the CSV header row
 *   - Skips lines that do not have exactly 5 fields
 *   - Skips records with unparseable age or income
 *   - Skips records with age ≤ 0 or age > 150
 *   - Skips records with income < 0
 *   - Skips records with age < 18 (below the first tracked group)
 */
public class AgeGroupMapper extends Mapper<Object, Text, Text, IncomeCountWritable> {

    // ---------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------
    private static final int FIELD_COUNT   = 5;
    private static final int IDX_AGE       = 1;
    private static final int IDX_INCOME    = 2;
    private static final int MIN_VALID_AGE = 1;
    private static final int MAX_VALID_AGE = 150;

    // ---------------------------------------------------------------
    // Reusable output objects — avoids per-record object allocation
    // and reduces GC pressure on large datasets.
    // ---------------------------------------------------------------
    private final Text                outKey   = new Text();
    private final IncomeCountWritable outValue = new IncomeCountWritable();

    // ---------------------------------------------------------------
    // Map
    // ---------------------------------------------------------------
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Skip empty lines and the CSV header row
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

        // ── Determine age-group label ─────────────────────────────────
        String ageGroup = getAgeGroup(age);
        if (ageGroup == null) {
            // Age is valid but below the first tracked group (< 18)
            context.getCounter("DataQuality", "UnderageSkipped").increment(1);
            return;
        }

        // ── Build IncomeCountWritable for this single record ──────────
        // A single record is its own min and max; count = 1; sumAge = age.
        outKey.set(ageGroup);
        outValue.setSumIncome(income);
        outValue.setCount(1L);
        outValue.setSumAge((long) age);
        outValue.setMinIncome(income);
        outValue.setMaxIncome(income);

        context.write(outKey, outValue);
        context.getCounter("DataQuality", "ValidRecords").increment(1);
    }

    // ---------------------------------------------------------------
    // Helper: numeric age → age-group label
    // ---------------------------------------------------------------

    /**
     * Maps a validated age to one of five age-group bucket labels, or
     * returns {@code null} when the age is below the first tracked group.
     *
     * <pre>
     *   18 – 24  →  "18-24"
     *   25 – 34  →  "25-34"
     *   35 – 44  →  "35-44"
     *   45 – 54  →  "45-54"
     *   55 +     →  "55+"
     *   < 18     →  null   (caller increments UnderageSkipped counter)
     * </pre>
     *
     * The labels must stay in sync with the switch-cases in
     * {@link AgeGroupPartitioner#getPartition}.
     *
     * @param age a pre-validated age in the range [MIN_VALID_AGE, MAX_VALID_AGE]
     * @return age-group label string, or {@code null} if age < 18
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
