import MapperClasses.SortUserMapClass;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Richard on 2/24/17.
 */
public class MapReduceDriver extends Configured implements Tool {

    private static HashMap<String, String> movieList = new HashMap<String, String>();
    private static ArrayList<String> topTenMoviesList;
    private static ArrayList<Integer> topTenUsersList;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapReduceDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        // First that does the first counting and lists avg ratings and number
        Job avgRatingJob = new Job();
        avgRatingJob.setJarByClass(MapReduceDriver.class);
        avgRatingJob.setJobName("WordCounter");

        FileInputFormat.addInputPath(avgRatingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(avgRatingJob, new Path(args[1] + "/ratingsOutput"));

        avgRatingJob.setOutputKeyClass(Text.class);
        avgRatingJob.setOutputValueClass(FloatWritable.class);
        avgRatingJob.setOutputFormatClass(TextOutputFormat.class);

        avgRatingJob.setMapperClass(MapperClasses.RatingsMapClass.class);
        avgRatingJob.setReducerClass(ReducerClasses.RatingsReduceClass.class);

        int returnValue = avgRatingJob.waitForCompletion(true) ? 0 : 1;
        if(avgRatingJob.isSuccessful()) {
            System.out.println("rating job was successful");
        } else if(!avgRatingJob.isSuccessful()) {
            System.out.println("rating job was not successful");
            return returnValue;
        }

        // second job to sort the ratings descending
        Job sortByRatingJob = new Job();
        sortByRatingJob.setJarByClass(MapReduceDriver.class);
        sortByRatingJob.setJobName("RatingSorter");

        FileInputFormat.addInputPath(sortByRatingJob, new Path(args[1] + "/ratingsOutput/part-r-00000" ));
        FileOutputFormat.setOutputPath(sortByRatingJob, new Path(args[1] + "/sortedRatingOutput"));

        sortByRatingJob.setSortComparatorClass(SortFloatComparator.class);

        sortByRatingJob.setOutputKeyClass(FloatWritable.class);
        sortByRatingJob.setOutputValueClass(Text.class);
        sortByRatingJob.setOutputFormatClass(TextOutputFormat.class);
        sortByRatingJob.setMapperClass(MapperClasses.SortRatingsMapClass.class);

        returnValue = sortByRatingJob.waitForCompletion(true) ? 0: 1;
        if(sortByRatingJob.isSuccessful()) {
            System.out.println("sort rating job was successful");
        } else if(!sortByRatingJob.isSuccessful()) {
            System.out.println("sort rating job was not successful");
        }

        // third job to mapreduce users
        Job userJob = new Job();
        userJob.setJarByClass(MapReduceDriver.class);
        userJob.setJobName("UserSorter");

        FileInputFormat.addInputPath(userJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1] + "/UserOutput"));

        userJob.setOutputKeyClass(LongWritable.class);
        userJob.setOutputValueClass(LongWritable.class);
        userJob.setOutputFormatClass(TextOutputFormat.class);

        userJob.setMapperClass(MapperClasses.UserMapClass.class);
        userJob.setReducerClass(ReducerClasses.UserReduceClass.class);

        returnValue = userJob.waitForCompletion(true) ? 0: 1;
        if(userJob.isSuccessful()) {
            System.out.println("user job was successful");
        } else if(!userJob.isSuccessful()) {
            System.out.println("user job was not successful");
        }

        // fourth job to sort the users descending
        Job sortByUserJob = new Job();
        sortByUserJob.setJarByClass(MapReduceDriver.class);
        sortByUserJob.setJobName("UserSorter");

        FileInputFormat.addInputPath(sortByUserJob, new Path(args[1] + "/UserOutput/part-r-00000"));
        FileOutputFormat.setOutputPath(sortByUserJob, new Path(args[1] + "/sortedUserOutput"));

        sortByUserJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        sortByUserJob.setOutputKeyClass(LongWritable.class);
        sortByUserJob.setOutputValueClass(LongWritable.class);
        sortByUserJob.setOutputFormatClass(TextOutputFormat.class);

        sortByUserJob.setMapperClass(SortUserMapClass.class);

        returnValue = sortByUserJob.waitForCompletion(true) ? 0: 1;
        if(sortByUserJob.isSuccessful()) {
            System.out.println("sorting by user job was successful");
        } else if(!sortByUserJob.isSuccessful()) {
            System.out.println("sorting by user job was not successful");
        }

        hashMovieList();        // puts all movies in a hashmap

        topTenMoviesList = getTopTenMovies();      // gets the top ten movies
        topTenUsersList = getTopTenUsers();         // gets the top ten users

        writeTopTen();          // writes the top ten movies and users in a file

        return returnValue;
    }

    public static void hashMovieList() {
        String file = "a3-dataset/movie_titles.txt";
        String line = "";
        String[] tokens;
        String id = "";
        String title = "";

        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);

            while((line = br.readLine()) != null) {
                tokens = line.split(",");
                id = tokens[0];

                for(int i = 2; i < tokens.length; i++) {
                    title += tokens[i];
                }
                movieList.put(id, title);
                title = "";
            }
            fileReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("unable to open file");
        } catch (IOException e) {
            System.out.println("Error reading file");
        }
    }

    public static ArrayList<String> getTopTenMovies() {
        ArrayList<String> topTenList = new ArrayList<String>();
        File file = new File("output/sortedRatingOutput/part-r-00000");
        String line = "";
        String[] tokens;
        int i = 0;

        try {
            FileReader topTenRatingsFileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(topTenRatingsFileReader);

            while((line = br.readLine()) != null && i < 10) {
                tokens = line.split("\t");

                topTenList.add(movieList.get(tokens[1]));
//                System.out.println(movieList.get(tokens[1]));
                i++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("unable to open file");
        } catch (IOException e) {
            System.out.println("Error reading file");
        }
        return topTenList;
    }

    public static ArrayList<Integer> getTopTenUsers() {
        ArrayList<Integer> topTenList = new ArrayList<Integer>();
        File file = new File("output/sortedUserOutput/part-r-00000");
        String line = "";
        String[] tokens;
        int i = 0;

        try {
            FileReader topTenRatingsFileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(topTenRatingsFileReader);

            while((line = br.readLine()) != null && i < 10) {
                tokens = line.split("\t");

                topTenList.add(Integer.parseInt(tokens[1]));
//                System.out.println(movieList.get(tokens[1]));
                i++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("unable to open file");
        } catch (IOException e) {
            System.out.println("Error reading file");
        }
        return topTenList;
    }

    public static void writeTopTen() {
        File file = new File("TopTenList.txt");

        try {
            // creates the file
            file.createNewFile();

            FileWriter writer = new FileWriter(file);

            System.out.println("Top 10 Movies");
            writer.write("Top 10 Movies\n");
            for(int i = 0, count = 1; i < topTenMoviesList.size(); i++, count++) {
                System.out.println(count + ") " + topTenMoviesList.get(i));
                writer.write(count + ") " + topTenMoviesList.get(i) + "\n");
            }

            System.out.println();
            writer.write("\n");

            System.out.println("Top 10 Users");
            writer.write("Top 10 Users\n");
            for(int i = 0, count = 1; i < topTenUsersList.size(); i++, count++) {
                System.out.println(count + ") " + topTenUsersList.get(i));
                writer.write(count + ") " + topTenUsersList.get(i) + "\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.out.println("file could not be created");
        }

    }
}
