
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
// Taken from https://www.javacodegeeks.com/2013/01/mapreduce-algorithms-secondary-sorting.html


// Here, the composite keys must be grouped on the natural key which is the term or the docid depending on the type of K,V pair 

public class CustomGroupingComparator extends WritableComparator {
    public CustomGroupingComparator() {
        super(CompositeKeyWritable.class, true);
    }
    @Override
    public int compare(WritableComparable term1, WritableComparable term2) {
    	CompositeKeyWritable natural_key1 = (CompositeKeyWritable) term1;
    	CompositeKeyWritable natural_key2 = (CompositeKeyWritable) term2;
        return natural_key1.getNaturalKey().compareTo(natural_key2.getNaturalKey());
    }
}
