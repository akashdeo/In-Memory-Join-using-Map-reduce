/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hw1q4;

import static com.sun.jersey.core.header.LinkHeader.uri;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akash
 */
public class Q4 {

    public static class mapIt extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        /**
         *
         * @param context
         * @throws IOException
         */
        HashMap<String, String> hmap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI mappingFileUri = null;
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            mappingFileUri = context.getCacheFiles()[0];
            Path getPath = new Path(mappingFileUri.getPath());
            BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
            String setupData = null;
            while ((setupData = bf.readLine()) != null) {
                String[] values = setupData.split("::");
                String address = values[1];
                String businessId = null;
                if (address.contains(("Palo Alto")) || address.contains(("palo alto"))) {
                    businessId = values[0];
                }
                hmap.put(businessId, values[2]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("::");
            String userId = values[1];
            String rating = values[3];
            String id = values[2];
            for (Map.Entry<String, String> entry : hmap.entrySet()) {
                String businessId = entry.getKey();
                if (id.equals(businessId)) {
                    //String answer = userId+","+rating;
                    context.write(new Text(userId), new DoubleWritable(Double.parseDouble(rating)));
                }
            }

        }
    }

    public static class reduceIt extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double avg;
            Double sum = 0.0;
            Double count = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count += 1;
            }
            avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Palo Alto");
        job.setJarByClass(Q4.class);
        job.setMapperClass(mapIt.class);
        job.setReducerClass(reduceIt.class);
        job.addCacheFile(new Path(args[1]).toUri());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
