import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AgeGroupReducer
 *
 * Receives all mapper (or combiner) records for a single age-group
 * partition and computes:
 *
 *   • Average income
 *   • Minimum income
 *   • Maximum income
 *   • Total persons (count)
 *   • Average age within the group
 *
 * ─── Input value formats ─────────────────────────────────────────────
 *
 *  Without Combiner  →  "age,income"
 *                        e.g.  "25,3000.0"
 *
 *  With Combiner     →  "C:sumAge,sumIncome,count,minIncome,maxIncome"
 *                        e.g.  "C:103,12000.0,4,3000.0,5000.0"
 *
 * The reducer detects the "C:" prefix (defined in AgeGroupCombiner) and
 * handles both formats transparently.
 *
 * ─── Sample output ───────────────────────────────────────────────────
 *
 *  25-34    Avg Income: 4500 | Min: 3000 | Max: 5500 | Count: 4 | Avg Age: 29
 */
public class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long   count      = 0;
        double sumIncome  = 0.0;
        double minIncome  = Double.MAX_VALUE;
        double maxIncome  = Double.MIN_VALUE;
        long   sumAge     = 0;

        for (Text val : values) {
            String raw = val.toString();

            // ── Detect combiner-produced value ────────────────────────
            if (raw.startsWith(AgeGroupCombiner.COMBINED_PREFIX)) {
                String[] p = raw.substring(AgeGroupCombiner.COMBINED_PREFIX.length()).split(",");
                if (p.length != 5) {
                    context.getCounter("ReducerDataQuality", "MalformedCombinedValue").increment(1);
                    continue;
                }
                try {
                    sumAge    += Long.parseLong(p[0].trim());
                    sumIncome += Double.parseDouble(p[1].trim());
                    count     += Long.parseLong(p[2].trim());
                    double cMin = Double.parseDouble(p[3].trim());
                    double cMax = Double.parseDouble(p[4].trim());
                    if (cMin < minIncome) minIncome = cMin;
                    if (cMax > maxIncome) maxIncome = cMax;
                } catch (NumberFormatException e) {
                    context.getCounter("ReducerDataQuality", "UnparsableCombinedValue").increment(1);
                }

            } else {
                // ── Raw mapper value: "age,income" ────────────────────
                String[] parts = raw.split(",");
                if (parts.length != 2) {
                    context.getCounter("ReducerDataQuality", "MalformedRawValue").increment(1);
                    continue;
                }
                try {
                    int    age    = Integer.parseInt(parts[0].trim());
                    double income = Double.parseDouble(parts[1].trim());
                    count++;
                    sumAge    += age;
                    sumIncome += income;
                    if (income < minIncome) minIncome = income;
                    if (income > maxIncome) maxIncome = income;
                } catch (NumberFormatException e) {
                    context.getCounter("ReducerDataQuality", "UnparsableRawValue").increment(1);
                }
            }
        }

        // ── Guard: no valid records received ─────────────────────────
        if (count == 0) {
            context.getCounter("ReducerDataQuality", "EmptyGroups").increment(1);
            return;
        }

        long avgIncome = Math.round(sumIncome / count);
        long avgAge    = Math.round((double) sumAge / count);

        String output = String.format(
                "Avg Income: %d | Min: %.0f | Max: %.0f | Count: %d | Avg Age: %d",
                avgIncome, minIncome, maxIncome, count, avgAge);

        result.set(output);
        context.write(key, result);
    }
}