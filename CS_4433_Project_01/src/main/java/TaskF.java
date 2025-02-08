import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskF {

    // Mapper for friends.csv
    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header
            if (fields[0].equals("FriendRel")) return;

            // Emit: key = PersonID, value = "F,MyFriend"
            String personId = fields[1];
            String friendId = fields[2];
            context.write(new Text(personId), new Text("F," + friendId));
        }
    }

    // Mapper for access_logs.csv
    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header
            if (fields[0].equals("AccessID")) return;

            // Emit: key = ByWho, value = "A,WhatPage"
            String personId = fields[1];
            String accessedPageId = fields[2];
            context.write(new Text(personId), new Text("A," + accessedPageId));
        }
    }

    public static class PersonMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip header
            if (fields[0].equals("PersonID")) return;

            // Emit: key = PersonID, value = "P,Name"
            String personId = fields[0];
            String name = fields[1];
            context.write(new Text(personId), new Text("P," + name));
        }
    }

    public static class FriendAccessReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> declaredFriends = new HashSet<>();
            Set<String> accessedPages = new HashSet<>();
            String personName = null;

            // Process all values
            for (Text val : values) {
                String[] parts = val.toString().split(",", 2);
                if (parts[0].equals("F")) {
                    // This is a friend relationship
                    declaredFriends.add(parts[1]);
                } else if (parts[0].equals("A")) {
                    // This is a page access
                    accessedPages.add(parts[1]);
                } else if (parts[0].equals("P")) {
                    // This is person's name
                    personName = parts[1];
                }
            }

            if (personName != null) {
                // Find friends whose pages were never accessed
                for (String friendId : declaredFriends) {
                    if (!accessedPages.contains(friendId)) {
                        // Emit PersonID and Name of person who hasn't accessed their friend's page
                        context.write(key, new Text(personName));
                    }
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friend Page Access Analysis");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(FriendAccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, PersonMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friend Page Access Analysis");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(FriendAccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, PersonMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}