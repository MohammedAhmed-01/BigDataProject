import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * IncomeCountWritable
 *
 * A custom Hadoop {@link Writable} that aggregates demographic statistics
 * for a single age-group partition.
 *
 * This class is designed to work alongside the existing Text-based pipeline
 * (AgeGroupMapper, AgeGroupCombiner, AgeGroupReducer, AgeGroupPartitioner,
 * AgeGroupDriver) without modifying any of those files.  It can be adopted
 * incrementally — for example, as the value type in a future refactor that
 * replaces the "C:<fields>" string encoding with a strongly-typed Writable.
 *
 * ─── Fields ──────────────────────────────────────────────────────────
 *
 *   sumIncome  – running total of all income values seen so far
 *   count      – number of persons aggregated
 *   sumAge     – running total of all age values (used to compute avg age)
 *   minIncome  – lowest income value encountered
 *   maxIncome  – highest income value encountered
 *
 * ─── Serialisation contract ───────────────────────────────────────────
 *
 *   write()     serialises fields in declaration order using DataOutput
 *   readFields() deserialises in the same order using DataInput
 *
 *   Both methods must remain in sync; changing field order is a breaking
 *   change for any data already written to HDFS.
 *
 * ─── Thread safety ────────────────────────────────────────────────────
 *
 *   Not thread-safe.  Hadoop reuses Writable instances within a single
 *   task JVM on one thread, so this is the expected contract.
 */
public class IncomeCountWritable implements Writable {

    // ---------------------------------------------------------------
    // Fields
    // ---------------------------------------------------------------

    /** Sum of all income values aggregated into this object. */
    private double sumIncome;

    /** Number of individual records merged into this object. */
    private long count;

    /** Sum of all age values aggregated into this object. */
    private long sumAge;

    /** Minimum income value seen across all merged records. */
    private double minIncome;

    /** Maximum income value seen across all merged records. */
    private double maxIncome;

    // ---------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------

    /**
     * Default no-argument constructor required by Hadoop's reflection-
     * based deserialisation.  Initialises the object to a "zero / empty"
     * state so that it is safe to call {@link #readFields} immediately
     * after construction.
     */
    public IncomeCountWritable() {
        this.sumIncome = 0.0;
        this.count     = 0L;
        this.sumAge    = 0L;
        this.minIncome = Double.MAX_VALUE;
        this.maxIncome = -Double.MAX_VALUE;  // use -MAX_VALUE, not MIN_VALUE
    }

    /**
     * Parameterised constructor for creating a fully populated instance
     * directly — useful when converting a single raw record (age, income)
     * into an {@code IncomeCountWritable} for the first time.
     *
     * @param sumIncome  initial income sum (often just one income value)
     * @param count      initial record count (often 1)
     * @param sumAge     initial age sum (often just one age value)
     * @param minIncome  initial minimum income
     * @param maxIncome  initial maximum income
     */
    public IncomeCountWritable(double sumIncome, long count, long sumAge,
                               double minIncome, double maxIncome) {
        this.sumIncome = sumIncome;
        this.count     = count;
        this.sumAge    = sumAge;
        this.minIncome = minIncome;
        this.maxIncome = maxIncome;
    }

    // ---------------------------------------------------------------
    // Writable interface — serialisation
    // ---------------------------------------------------------------

    /**
     * Serialises this object's fields to {@code out} in a fixed order.
     * Hadoop calls this method when writing records to the shuffle/sort
     * intermediate files or to HDFS output.
     *
     * Field write order:
     *   1. sumIncome  (double)
     *   2. count      (long)
     *   3. sumAge     (long)
     *   4. minIncome  (double)
     *   5. maxIncome  (double)
     *
     * @param out the data output stream provided by the Hadoop framework
     * @throws IOException if an I/O error occurs during serialisation
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sumIncome);
        out.writeLong(count);
        out.writeLong(sumAge);
        out.writeDouble(minIncome);
        out.writeDouble(maxIncome);
    }

    /**
     * Deserialises this object's fields from {@code in}.  Must read in
     * exactly the same order as {@link #write(DataOutput)}.
     *
     * Hadoop reuses Writable instances across records; this method
     * overwrites all fields, so stale state from a previous record is
     * never visible after a successful read.
     *
     * @param in the data input stream provided by the Hadoop framework
     * @throws IOException if an I/O error occurs during deserialisation
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.sumIncome = in.readDouble();
        this.count     = in.readLong();
        this.sumAge    = in.readLong();
        this.minIncome = in.readDouble();
        this.maxIncome = in.readDouble();
    }

    // ---------------------------------------------------------------
    // Merge
    // ---------------------------------------------------------------

    /**
     * Merges another {@code IncomeCountWritable} into this instance,
     * accumulating its statistics.  This is the equivalent of what the
     * existing {@code AgeGroupCombiner} does with the "C:" string format,
     * expressed as a type-safe operation.
     *
     * <p>After merging:
     * <ul>
     *   <li>{@code this.count}     += {@code other.count}</li>
     *   <li>{@code this.sumIncome} += {@code other.sumIncome}</li>
     *   <li>{@code this.sumAge}    += {@code other.sumAge}</li>
     *   <li>{@code this.minIncome}  = min(this.minIncome, other.minIncome)</li>
     *   <li>{@code this.maxIncome}  = max(this.maxIncome, other.maxIncome)</li>
     * </ul>
     *
     * <p>Merging an object with {@code count == 0} is a no-op for all
     * statistical fields (though sumAge / sumIncome will still be added,
     * which is harmless since they will be 0).
     *
     * @param other the {@code IncomeCountWritable} to merge into this one;
     *              must not be {@code null}
     * @throws IllegalArgumentException if {@code other} is {@code null}
     */
    public void merge(IncomeCountWritable other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot merge a null IncomeCountWritable.");
        }
        this.count     += other.count;
        this.sumIncome += other.sumIncome;
        this.sumAge    += other.sumAge;
        if (other.minIncome < this.minIncome) this.minIncome = other.minIncome;
        if (other.maxIncome > this.maxIncome) this.maxIncome = other.maxIncome;
    }

    // ---------------------------------------------------------------
    // Derived statistics (computed on demand, not stored)
    // ---------------------------------------------------------------

    /**
     * Returns the average income, or {@code 0.0} if no records have been
     * aggregated ({@code count == 0}).
     *
     * @return average income
     */
    public double getAverageIncome() {
        return count == 0 ? 0.0 : sumIncome / count;
    }

    /**
     * Returns the average age, or {@code 0.0} if no records have been
     * aggregated ({@code count == 0}).
     *
     * @return average age
     */
    public double getAverageAge() {
        return count == 0 ? 0.0 : (double) sumAge / count;
    }

    // ---------------------------------------------------------------
    // Getters
    // ---------------------------------------------------------------

    /**
     * @return the running sum of all income values merged into this object
     */
    public double getSumIncome() {
        return sumIncome;
    }

    /**
     * @return the total number of records merged into this object
     */
    public long getCount() {
        return count;
    }

    /**
     * @return the running sum of all age values merged into this object
     */
    public long getSumAge() {
        return sumAge;
    }

    /**
     * @return the minimum income value seen across all merged records,
     *         or {@link Double#MAX_VALUE} if no records have been merged
     */
    public double getMinIncome() {
        return minIncome;
    }

    /**
     * @return the maximum income value seen across all merged records,
     *         or {@code -}{@link Double#MAX_VALUE} if no records have been merged
     */
    public double getMaxIncome() {
        return maxIncome;
    }

    // ---------------------------------------------------------------
    // Setters
    // ---------------------------------------------------------------

    /**
     * Replaces the current income sum.
     *
     * @param sumIncome new income sum value
     */
    public void setSumIncome(double sumIncome) {
        this.sumIncome = sumIncome;
    }

    /**
     * Replaces the current record count.
     *
     * @param count new count value; must be &gt;= 0
     */
    public void setCount(long count) {
        this.count = count;
    }

    /**
     * Replaces the current age sum.
     *
     * @param sumAge new age sum value
     */
    public void setSumAge(long sumAge) {
        this.sumAge = sumAge;
    }

    /**
     * Replaces the current minimum income.
     *
     * @param minIncome new minimum income value
     */
    public void setMinIncome(double minIncome) {
        this.minIncome = minIncome;
    }

    /**
     * Replaces the current maximum income.
     *
     * @param maxIncome new maximum income value
     */
    public void setMaxIncome(double maxIncome) {
        this.maxIncome = maxIncome;
    }

    // ---------------------------------------------------------------
    // toString
    // ---------------------------------------------------------------

    /**
     * Returns a human-readable summary of the aggregated statistics,
     * mirroring the output format produced by {@code AgeGroupReducer} so
     * that the two representations can be compared or used interchangeably
     * in logging and unit tests.
     *
     * <p>Example output:
     * <pre>
     *   Avg Income: 4500 | Min: 3000 | Max: 5500 | Count: 4 | Avg Age: 29
     * </pre>
     *
     * @return formatted statistics string
     */
    @Override
    public String toString() {
        if (count == 0) {
            return "Avg Income: N/A | Min: N/A | Max: N/A | Count: 0 | Avg Age: N/A";
        }
        return String.format(
                "Avg Income: %d | Min: %.0f | Max: %.0f | Count: %d | Avg Age: %d",
                Math.round(getAverageIncome()),
                minIncome,
                maxIncome,
                count,
                Math.round(getAverageAge()));
    }
}
