package ReducerClasses;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Richard on 2/24/17.
 */
public class RatingsReduceClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        float avg;

        Iterator<FloatWritable> valuesIt = values.iterator();

        while(valuesIt.hasNext()) {
            sum += valuesIt.next().get();
            count++;
        }

        avg = sum / count;

        context.write(key, new FloatWritable(avg));
    }
}
