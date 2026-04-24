package agegroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * AgeGroupMapper - Task 15: Age Group Partitioning
 *
 * Member 1 Responsibilities:
 *   - Parse and validate input records
 *   - Extract key-value pairs (ageGroup → income)
 *   - Apply all validation rules
 *
 * Input format (CSV):
 *   person_id, age, income, employment_status, education_level
 *   Example: P001,25,3000,Employed,Bachelor
 *
 * Output:
 *   Key   → Text  : age group label  (e.g. "20-30")
 *   Value → Text  : income|employment_status  (e.g. "3000|Employed")
 *
 * Age group ranges (must match AgeGroupPartitioner):
 *   "Under 20"  →  age < 20
 *   "20-30"     →  20 <= age <= 30
 *   "31-50"     →  31 <= age <= 50
 *   "51+"       →  age >= 51
 */
public class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {

    // ---------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------
    private static final Logger LOG = Logger.getLogger(AgeGroupMapper.class);

    private static final int MIN_VALID_AGE    = 1;
    private static final int MAX_VALID_AGE    = 120;
    private static final int MIN_VALID_INCOME = 0;

    private static final int FIELD_PERSON_ID   = 0;
    private static final int FIELD_AGE         = 1;
    private static final int FIELD_INCOME      = 2;
    private static final int FIELD_EMP_STATUS  = 3;
    // FIELD_EDUCATION = 4  (index 4, not used in output but validated)
    private static final int EXPECTED_FIELDS   = 5;

    // ---------------------------------------------------------------
    // Counters  (visible in the Hadoop job counters UI)
    // ---------------------------------------------------------------
    enum ValidationCounter {
        RECORDS_PROCESSED,
        RECORDS_SKIPPED_BLANK,
        RECORDS_SKIPPED_WRONG_FIELD_COUNT,
        RECORDS_SKIPPED_INVALID_AGE,
        RECORDS_SKIPPED_INVALID_INCOME,
        RECORDS_SKIPPED_MISSING_STATUS
    }

    // ---------------------------------------------------------------
    // Reusable output objects (avoid GC pressure on large files)
    // ---------------------------------------------------------------
    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    // ---------------------------------------------------------------
    // map()
    // ---------------------------------------------------------------
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        String raw = line.toString().trim();

        // --- 1. Skip blank lines and header rows ---
        if (raw.isEmpty() || raw.startsWith("person_id") || raw.startsWith("#")) {
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_BLANK).increment(1);
            return;
        }

        // --- 2. Split and check field count ---
        String[] fields = raw.split(",", -1);   // -1 keeps trailing empty tokens
        if (fields.length < EXPECTED_FIELDS) {
            LOG.warn("Skipping record – expected " + EXPECTED_FIELDS
                    + " fields but found " + fields.length + " | line: " + raw);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_WRONG_FIELD_COUNT).increment(1);
            return;
        }

        String personId        = fields[FIELD_PERSON_ID].trim();
        String ageStr          = fields[FIELD_AGE].trim();
        String incomeStr       = fields[FIELD_INCOME].trim();
        String employmentStatus = fields[FIELD_EMP_STATUS].trim();

        // --- 3. Validate age ---
        int age;
        try {
            age = Integer.parseInt(ageStr);
        } catch (NumberFormatException e) {
            LOG.warn("Skipping record – non-integer age '" + ageStr
                    + "' for person " + personId);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_AGE).increment(1);
            return;
        }

        if (age < MIN_VALID_AGE || age > MAX_VALID_AGE) {
            LOG.warn("Skipping record – age out of valid range [" + MIN_VALID_AGE
                    + "-" + MAX_VALID_AGE + "]: " + age + " for person " + personId);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_AGE).increment(1);
            return;
        }

        // --- 4. Validate income ---
        int income;
        try {
            income = Integer.parseInt(incomeStr);
        } catch (NumberFormatException e) {
            LOG.warn("Skipping record – non-integer income '" + incomeStr
                    + "' for person " + personId);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_INCOME).increment(1);
            return;
        }

        if (income < MIN_VALID_INCOME) {
            LOG.warn("Skipping record – negative income " + income
                    + " for person " + personId);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_INCOME).increment(1);
            return;
        }

        // --- 5. Validate employment status (must not be blank) ---
        if (employmentStatus.isEmpty()) {
            LOG.warn("Skipping record – empty employment status for person " + personId);
            context.getCounter(ValidationCounter.RECORDS_SKIPPED_MISSING_STATUS).increment(1);
            return;
        }

        // --- 6. Determine age group ---
        String ageGroup = getAgeGroup(age);

        // --- 7. Emit (ageGroup → income|employmentStatus) ---
        // The composite value lets the Reducer compute:
        //   - average income  (from income part)
        //   - employment rate (from employmentStatus part)
        outKey.set(ageGroup);
        outValue.set(income + "|" + employmentStatus);
        context.write(outKey, outValue);

        context.getCounter(ValidationCounter.RECORDS_PROCESSED).increment(1);
    }

    // ---------------------------------------------------------------
    // cleanup() – log summary counters
    // ---------------------------------------------------------------
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long processed = context.getCounter(ValidationCounter.RECORDS_PROCESSED).getValue();
        long skipped   = context.getCounter(ValidationCounter.RECORDS_SKIPPED_BLANK).getValue()
                       + context.getCounter(ValidationCounter.RECORDS_SKIPPED_WRONG_FIELD_COUNT).getValue()
                       + context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_AGE).getValue()
                       + context.getCounter(ValidationCounter.RECORDS_SKIPPED_INVALID_INCOME).getValue()
                       + context.getCounter(ValidationCounter.RECORDS_SKIPPED_MISSING_STATUS).getValue();

        LOG.info("AgeGroupMapper finished – processed: " + processed
                + ", skipped: " + skipped);
    }

    // ---------------------------------------------------------------
    // Helper: map an age to its group label
    // NOTE: These ranges MUST match the groups defined in AgeGroupPartitioner
    // ---------------------------------------------------------------
    /**
     * Returns the age group label for the given age.
     *
     * Groups:
     *   Under 20  →  age < 20
     *   20-30     →  20 <= age <= 30
     *   31-50     →  31 <= age <= 50
     *   51+       →  age >= 51
     *
     * @param age validated positive integer
     * @return age group label string
     */
    public static String getAgeGroup(int age) {
        if (age < 20)        return "Under 20";
        else if (age <= 30)  return "20-30";
        else if (age <= 50)  return "31-50";
        else                 return "51+";
    }
}