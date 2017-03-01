import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Richard on 2/26/17.
 */
public class SortFloatComparator<T> extends WritableComparator {

    //Constructor.
    protected SortFloatComparator() {
        super(FloatWritable.class, true);
    }

    @SuppressWarnings("rawtypes")

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        FloatWritable k1 = (FloatWritable)w1;
        FloatWritable k2 = (FloatWritable)w2;

        return -1 * k1.compareTo(k2);
    }
}
