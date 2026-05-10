import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * AgeGroupPartitioner
 *
 * Routes each Mapper output record to the correct Reducer task based on
 * the age-group key emitted by {@link AgeGroupMapper}.
 *
 * Type signature:
 *   Key   : Text                  (age-group label)
 *   Value : IncomeCountWritable   (unused by the partitioner — routing
 *                                  is done on the key alone, but the
 *                                  type parameter must match the Mapper's
 *                                  output value type to satisfy Hadoop's
 *                                  generic type checks)
 *
 * Partition mapping (5 age groups → 5 reducers):
 *   Partition 0  →  "18-24"
 *   Partition 1  →  "25-34"
 *   Partition 2  →  "35-44"
 *   Partition 3  →  "45-54"
 *   Partition 4  →  "55+"
 *
 * If an unrecognised key arrives (should not happen after mapper
 * validation) it falls back gracefully to a hash-based partition so
 * no record is lost.
 *
 * IMPORTANT: {@link #NUM_PARTITIONS} must equal the number of reduce
 * tasks configured in {@link AgeGroupDriver}.  The Driver enforces this.
 */
public class AgeGroupPartitioner extends Partitioner<Text, IncomeCountWritable> {

    /** Number of age-group partitions. Must match the reducer count in the Driver. */
    public static final int NUM_PARTITIONS = 5;

    // ---------------------------------------------------------------
    // getPartition
    // ---------------------------------------------------------------

    /**
     * Returns the zero-based partition index for the given key.
     *
     * <p>When {@code numReduceTasks} equals {@link #NUM_PARTITIONS}, the
     * switch provides a deterministic, label-stable mapping.  If the job
     * is ever launched with a different reducer count (e.g. during
     * testing), the method degrades to a consistent hash to avoid
     * losing records.
     *
     * @param key            age-group label Text writable
     * @param value          IncomeCountWritable (ignored — routing is key-only)
     * @param numReduceTasks number of reducer tasks configured in the Driver
     * @return partition index in the range [0, numReduceTasks)
     */
    @Override
    public int getPartition(Text key, IncomeCountWritable value, int numReduceTasks) {

        // Guard: if the reducer count has been changed, fall back to hash
        if (numReduceTasks != NUM_PARTITIONS) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }

        switch (key.toString()) {
            case "18-24": return 0;
            case "25-34": return 1;
            case "35-44": return 2;
            case "45-54": return 3;
            case "55+":   return 4;
            default:
                // Unknown group — route to first reducer rather than throwing
                return 0;
        }
    }
}
