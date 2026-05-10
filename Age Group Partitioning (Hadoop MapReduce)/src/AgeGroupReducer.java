import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AgeGroupReducer
 *
 * Receives all {@link IncomeCountWritable} values for a single age-group
 * partition (either directly from the Mapper, or pre-aggregated by the
 * Combiner) and computes the final statistics:
 *
 *   • Average income
 *   • Minimum income
 *   • Maximum income
 *   • Total person count
 *   • Average age within the group
 *
 * Type signature:
 *   Input  key   : Text                  (age-group label)
 *   Input  value : IncomeCountWritable   (from Mapper or Combiner)
 *   Output key   : Text                  (age-group label — passed through)
 *   Output value : Text                  (formatted statistics string)
 *
 * ─── How this works with and without the Combiner ────────────────────
 *
 *   Without Combiner:
 *     Each value is a single-record IncomeCountWritable (count=1).
 *     The reducer merges N such objects, one per CSV row.
 *
 *   With Combiner:
 *     Each value is a partially-aggregated IncomeCountWritable from a
 *     local node.  The reducer merges these partial aggregates.
 *
 *   In both cases the reducer logic is identical — it simply calls
 *   {@link IncomeCountWritable#merge} on every incoming value.
 *   The "C:" prefix detection that was needed in the Text-based
 *   implementation is no longer required.
 *
 * ─── Sample output line ──────────────────────────────────────────────
 *
 *   25-34    Avg Income: 4500 | Min: 3000 | Max: 5500 | Count: 4 | Avg Age: 29
 */
public class AgeGroupReducer
        extends Reducer<Text, IncomeCountWritable, Text, Text> {

    // Reusable output writable — one instance per Reducer task
    private final Text         result      = new Text();

    // Accumulator — reset at the start of every reduce() call
    private final IncomeCountWritable accumulator = new IncomeCountWritable();

    // ---------------------------------------------------------------
    // Reduce
    // ---------------------------------------------------------------

    /**
     * Merges all {@link IncomeCountWritable} values for {@code key} and
     * writes a single formatted statistics line.
     *
     * @param key     age-group label (e.g. "35-44")
     * @param values  iterable of IncomeCountWritable objects
     * @param context MapReduce task context for writing output and counters
     */
    @Override
    protected void reduce(Text key, Iterable<IncomeCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // ── Reset accumulator ─────────────────────────────────────────
        // Hadoop may reuse the same Reducer instance for multiple keys,
        // so the accumulator must be reset to a clean "empty" state.
        accumulator.setSumIncome(0.0);
        accumulator.setCount(0L);
        accumulator.setSumAge(0L);
        accumulator.setMinIncome(Double.MAX_VALUE);
        accumulator.setMaxIncome(-Double.MAX_VALUE);

        // ── Merge all values for this key ─────────────────────────────
        for (IncomeCountWritable val : values) {
            accumulator.merge(val);
        }

        // ── Guard: no valid records received ─────────────────────────
        if (accumulator.getCount() == 0) {
            context.getCounter("ReducerDataQuality", "EmptyGroups").increment(1);
            return;
        }

        // ── Compute final statistics ──────────────────────────────────
        long avgIncome = Math.round(accumulator.getAverageIncome());
        long avgAge    = Math.round(accumulator.getAverageAge());

        String output = String.format(
                "Avg Income: %d | Min: %.0f | Max: %.0f | Count: %d | Avg Age: %d",
                avgIncome,
                accumulator.getMinIncome(),
                accumulator.getMaxIncome(),
                accumulator.getCount(),
                avgAge);

        result.set(output);
        context.write(key, result);
    }
}
