import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;

public class NationalityCount {

    public static class CountryMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String name = fields[1].trim();
            String nationality = fields[2].trim();
            String hobby = fields[4].trim();
            if (nationality.equalsIgnoreCase("Netherlands")) {
                context.write(new Text(name), new Text(hobby));
            }
        }
    }
    public void debug(String[] args) throws Exception {}

    public static void main(String[] args) throws Exception {}
}


