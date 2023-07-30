package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jgrapht.Graph;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;


import com.fasterxml.jackson.databind.ser.std.StdKeySerializers;
import org.apache.commons.math.ode.sampling.StepInterpolator;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.jgrapht.*;
import org.jgrapht.alg.connectivity.*;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm.*;
import org.jgrapht.alg.interfaces.*;
import org.jgrapht.alg.shortestpath.*;
import org.jgrapht.graph.*;

import java.util.*;

import org.jgrapht.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.*;


public class Step3 {


    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        Text newKey = new Text();
        Text newVal = new Text();



        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith("total-"))
                context.write(new Text("total"), new Text("1"));
            else {
                String[] spt = value.toString().split(" ");
                newKey.set(spt[0]); // spt[0] = route
                newVal.set(key.toString() + " " + spt[1]);
                context.write(newKey, newVal);
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        Text newKey = new Text();
        Text newVal = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.toString().equals("total")) {
                int sum = 0;
                for (Text val : values) {
                    sum = sum + Integer.parseInt(val.toString());
                }
                context.write(new Text("total"), new Text("" + sum));
            } else {
                for (Text val : values) {
                    String[] spt = val.toString().split(" "); // "noun,noun sum"
                    newKey.set(spt[0]);
                    newVal.set(key.toString() + " " + spt[1]);
                    context.write(newKey, newVal);
                }

            }
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //JOB-3
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "step3");
        job.setJarByClass(org.example.Step3.class);
        job.setMapperClass(org.example.Step3.MapperClass.class);
        job.setPartitionerClass(org.example.Step3.PartitionerClass.class);
        //job.setCombinerClass(Step1.CombinerClass.class);
        job.setReducerClass(org.example.Step3.ReducerClass.class);


        //map output <key,value>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //should be args[1]
        job.waitForCompletion(true);

    }


}

