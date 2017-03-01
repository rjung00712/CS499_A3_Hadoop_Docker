package MapperClasses;

import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Richard on 2/24/17.
 */
public class RatingsMapClass extends Mapper<LongWritable, Text, Text, FloatWritable> {
//    private final static IntWritable one = new IntWritable(1);
    private Text movieId = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
        String[] tokens = line.split(",");

        String tokenizer = tokens[0];

        FloatWritable val = new FloatWritable(Float.parseFloat(tokens[2]));

//        while(tokenizer.hasMoreTokens()) {
            movieId.set(tokenizer);

            context.write(movieId, val);
    }
}

