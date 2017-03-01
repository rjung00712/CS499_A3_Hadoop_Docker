package ReducerClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Richard on 2/28/17.
 */
public class UserReduceClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        int sum = 0;
        long count;
        Iterator<LongWritable> i = values.iterator();
        while(i.hasNext()) {
            count = i.next().get();
            sum += count;
        }
        context.write(new LongWritable(sum), key);
    }
}
