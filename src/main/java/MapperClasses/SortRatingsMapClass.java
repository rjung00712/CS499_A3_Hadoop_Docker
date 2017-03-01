package MapperClasses;

import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Richard on 2/26/17.
 */
public class SortRatingsMapClass extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private Text movieId = new Text();
    private FloatWritable avgRating;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FloatWritable, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        avgRating = new FloatWritable(Float.parseFloat(tokens[1]));
        String id = tokens[0];

        movieId.set(id);

        context.write(avgRating, movieId);
    }
}
