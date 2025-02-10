import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskB {
    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (!fields[0].equals("AccessID")) {
                String pageID = fields[2].trim();
                context.write(new Text(pageID), new Text("A,1"));
            }
        }
    }

    public static class PersonMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (!fields[0].equals("PersonID")) {
                String personID = fields[0].trim();
                String name = fields[1].trim();
                String nationality = fields[2].trim();
                context.write(new Text(personID), new Text("P," + name + "," + nationality));
            }
        }
    }

    public static class CountReducer extends Reducer<Text, Text, IntWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int accessCount = 0;
            String name = null;
            String nationality = null;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("A")) {
                    accessCount += Integer.parseInt(parts[1]);
                } else if (parts[0].equals("P")) {
                    name = parts[1];
                    nationality = parts[2];
                }
            }

            if (name != null) {
                // negative to sort it in descending order because it by default sorts it in ascending order
                context.write(new IntWritable(-accessCount),
                        new Text(key + "\t" + name + "\t" + nationality));
            }
        }
    }

    // Second Job: Get top 10
    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", 2);
            // Input format: count   pageInfo
            int count = Integer.parseInt(parts[0].trim());
            context.write(new IntWritable(count), new Text(parts[1]));
        }
    }

    public static class Top10Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private int count = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int positiveCount = -key.get();

            for (Text val : values) {
                if (count < 10) {
                    context.write(new IntWritable(positiveCount), val);
                    count++;
                }
            }
        }

    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Facebook Pages");
        job1.setJarByClass(TaskB.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, PersonMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Sort Top 10 Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(Top10Reducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Facebook Pages");
        job1.setJarByClass(TaskB.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, PersonMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Sort Top 10 Pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(Top10Reducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

