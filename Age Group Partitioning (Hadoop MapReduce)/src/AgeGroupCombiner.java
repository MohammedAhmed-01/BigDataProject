import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AgeGroupCombiner
 *
 * Optional local combiner that runs on the same node as each Mapper to
 * reduce the volume of data sent across the network during the shuffle
 * phase, before records reach the Reducer.
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  Why a Combiner is valid here                                   │
 * │  ─────────────────────────────────────────────────────────────  │
 * │  All aggregations (sumIncome, count, sumAge, min, max) are      │
 * │  associative and commutative, so partial results produced by    │
 * │  the Combiner can safely be merged again by the Reducer.        │
 * │                                                                 │
 * │  The Combiner's input and output types are IDENTICAL            │
 * │  (Text key, IncomeCountWritable value), which satisfies the     │
 * │  Hadoop contract that the Combiner's output schema must match   │
 * │  the Mapper's output schema.                                    │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * Type signature:
 *   Input  key   : Text                  (age-group label from Mapper)
 *   Input  value : IncomeCountWritable   (per-record or already-combined)
 *   Output key   : Text                  (same age-group label)
 *   Output value : IncomeCountWritable   (merged partial aggregate)
 *
 * The "C:" string prefix used in the previous Text-based implementation
 * is no longer needed; the type system now distinguishes mapper values
 * from combined values unambiguously.
 *
 * ─────────────────────────────────────────────────────────────────────
 * Toggle via the driver property (default = enabled):
 *   -D agegroup.combiner=true
 *   -D agegroup.combiner=false
 * ─────────────────────────────────────────────────────────────────────
 */
public class AgeGroupCombiner
        extends Reducer<Text, IncomeCountWritable, Text, IncomeCountWritable> {

    // Reusable output object — one per Combiner task instance
    private final IncomeCountWritable outValue = new IncomeCountWritable();

    // ---------------------------------------------------------------
    // Reduce (local combine)
    // ---------------------------------------------------------------

    /**
     * Merges all {@link IncomeCountWritable} values for a given age-group
     * key that were produced by mappers on the same node, then emits a
     * single partially-aggregated {@link IncomeCountWritable}.
     *
     * <p>Hadoop may call this method multiple times on the same node for
     * the same key (e.g. if in-memory buffers are spilled more than once).
     * Because {@link IncomeCountWritable#merge} is associative and
     * commutative, repeated combining always produces a correct result.
     *
     * @param key     age-group label (e.g. "25-34")
     * @param values  iterable of per-record (or already-combined) writables
     * @param context MapReduce task context for writing output
     */
    @Override
    protected void reduce(Text key, Iterable<IncomeCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // Reset the accumulator to the "empty" state before processing
        // this key's values.  This is critical because Hadoop reuses the
        // same Combiner instance across multiple reduce() calls.
        outValue.setSumIncome(0.0);
        outValue.setCount(0L);
        outValue.setSumAge(0L);
        outValue.setMinIncome(Double.MAX_VALUE);
        outValue.setMaxIncome(-Double.MAX_VALUE);

        for (IncomeCountWritable val : values) {
            outValue.merge(val);
        }

        // Only emit if at least one valid record was merged
        if (outValue.getCount() == 0) {
            return;
        }

        context.write(key, outValue);
    }
}
