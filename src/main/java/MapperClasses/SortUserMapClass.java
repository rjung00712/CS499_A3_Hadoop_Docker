package MapperClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by Richard on 2/28/17.
 */
public class SortUserMapClass extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private LongWritable userId;
    private LongWritable numOfRatings;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        userId = new LongWritable(Integer.parseInt(tokens[0]));
        numOfRatings = new LongWritable(Integer.parseInt(tokens[1]));
        context.write(userId, numOfRatings);
    }
}