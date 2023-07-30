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


public class Step2 {


    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        Text newKey = new Text();
        Text newVal = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value); //< noun1,noun2 | route amount >
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        Text newVal = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String newValue = "";
            for (Text val : values) {
                newValue =newValue +"---"+val;
            }
            newVal.set(newValue.substring(3));
            context.write(key,newVal); //< noun1,noun2 | route1 amount1 route2 amount2 .....>


        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return 0;
        }

    }








    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //JOB-2
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "step2");
        job.setJarByClass(org.example.Step2.class);
        job.setMapperClass(org.example.Step2.MapperClass.class);
        job.setPartitionerClass(org.example.Step2.PartitionerClass.class);
        //job.setCombinerClass(Step1.CombinerClass.class);
        job.setReducerClass(org.example.Step2.ReducerClass.class);


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

