package MapperClasses;

import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Richard on 2/28/17.
 */
public class UserMapClass extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private LongWritable userId;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
        String[] tokens = line.split(",");

        userId = new LongWritable(Integer.parseInt(tokens[1]));

        context.write(userId, one);
    }
}