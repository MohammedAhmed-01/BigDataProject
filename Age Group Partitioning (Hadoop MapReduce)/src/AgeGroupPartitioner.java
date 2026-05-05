import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * AgeGroupPartitioner
 *
 * Routes each mapper output record to the correct reducer based on the
 * age-group key emitted by AgeGroupMapper.
 *
 * Partition mapping (5 age groups → 5 reducers):
 *   Partition 0 → "18-24"
 *   Partition 1 → "25-34"
 *   Partition 2 → "35-44"
 *   Partition 3 → "45-54"
 *   Partition 4 → "55+"
 *
 * If an unrecognised key arrives (should not happen after mapper
 * validation) it falls into partition 0 as a safe default.
 *
 * IMPORTANT: numReduceTasks passed to this method is the value set in
 * the Driver.  It must equal NUM_PARTITIONS (5) — the Driver enforces
 * this.  If the job is ever run with a different reducer count the
 * method degrades gracefully using Math.abs(hashCode) % numReduceTasks.
 */
public class AgeGroupPartitioner extends Partitioner<Text, Text> {

    /** Must match the number of reducers set in AgeGroupDriver. */
    public static final int NUM_PARTITIONS = 5;

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        // Guard: if someone changes reducer count, fall back to hash
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
                // Unknown group — route to first reducer
                return 0;
        }
    }
}