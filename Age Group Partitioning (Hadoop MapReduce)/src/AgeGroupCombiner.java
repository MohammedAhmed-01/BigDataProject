import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AgeGroupCombiner
 *
 * Optional local combiner that runs on the same node as each Mapper to
 * reduce network shuffle volume before records reach the Reducer.
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  Why a Combiner is valid here                                   │
 * │  ─────────────────────────────────────────────────────────────  │
 * │  The final aggregations (sum of incomes, count, sum of ages,    │
 * │  min, max) are all associative and commutative, so partial      │
 * │  results from the Combiner can safely be merged by the Reducer. │
 * │  The Combiner emits the SAME key/value schema as the Mapper so  │
 * │  the Reducer code path is identical.                            │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * Combiner output format (same as mapper output):
 *   key   = age-group label
 *   value = "avgAge_so_far,sumIncome,count,minIncome,maxIncome"
 *           encoded as a single comma-separated string
 *
 * Because the value schema changes after combining, the Reducer must
 * detect which format it receives.  To keep things simple, the Combiner
 * emits a TAGGED value:
 *
 *   "C:<sumAge>,<sumIncome>,<count>,<minIncome>,<maxIncome>"
 *
 * The Reducer checks for the "C:" prefix and adjusts parsing accordingly.
 * Plain (non-combined) values keep the "age,income" format and are
 * handled by the same reducer logic.
 *
 * ─────────────────────────────────────────────────────────────────────
 * To toggle the Combiner on/off, set the driver property:
 *   -D agegroup.combiner=true|false   (default = true)
 * ─────────────────────────────────────────────────────────────────────
 */
public class AgeGroupCombiner extends Reducer<Text, Text, Text, Text> {

    /** Prefix that distinguishes a combined value from a raw mapper value. */
    public static final String COMBINED_PREFIX = "C:";

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long   count     = 0;
        double sumIncome = 0.0;
        long   sumAge    = 0;
        double minIncome = Double.MAX_VALUE;
        double maxIncome = Double.MIN_VALUE;

        for (Text val : values) {
            String raw = val.toString();

            if (raw.startsWith(COMBINED_PREFIX)) {
                // ── Already-combined record (rare but possible if Hadoop
                //    calls the Combiner multiple times on the same node) ──
                String[] p = raw.substring(COMBINED_PREFIX.length()).split(",");
                if (p.length != 5) continue;
                try {
                    sumAge    += Long.parseLong(p[0].trim());
                    sumIncome += Double.parseDouble(p[1].trim());
                    count     += Long.parseLong(p[2].trim());
                    double cMin = Double.parseDouble(p[3].trim());
                    double cMax = Double.parseDouble(p[4].trim());
                    if (cMin < minIncome) minIncome = cMin;
                    if (cMax > maxIncome) maxIncome = cMax;
                } catch (NumberFormatException ignored) { /* skip */ }

            } else {
                // ── Raw mapper value: "age,income" ────────────────────
                String[] p = raw.split(",");
                if (p.length != 2) continue;
                try {
                    int    age    = Integer.parseInt(p[0].trim());
                    double income = Double.parseDouble(p[1].trim());
                    count++;
                    sumAge    += age;
                    sumIncome += income;
                    if (income < minIncome) minIncome = income;
                    if (income > maxIncome) maxIncome = income;
                } catch (NumberFormatException ignored) { /* skip */ }
            }
        }

        if (count == 0) return;

        // Emit aggregated partial result tagged with "C:"
        String combined = COMBINED_PREFIX
                + sumAge    + ","
                + sumIncome + ","
                + count     + ","
                + minIncome + ","
                + maxIncome;

        outValue.set(combined);
        context.write(key, outValue);
    }
}