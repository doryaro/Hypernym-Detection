package org.example;

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

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        Text newKey = new Text();
        Text newVal = new Text();
        int dpMin = 30;

        englishStemmer stemmer = new englishStemmer();

        private static Graph<String, DefaultEdge> buildEmptySimpleGraph() {
            return GraphTypeBuilder
                    .<String, DefaultEdge>directed().allowingMultipleEdges(false)
                    .allowingSelfLoops(false).edgeClass(DefaultEdge.class).weighted(false).buildGraph();
        }

        public boolean validNoun(String word) {
            word = word.toLowerCase();
            for (int i = 0; i < word.length(); i++) {
                if (word.charAt(i) < 'a' || word.charAt(i) > 'z')
                    return false;

            }
            return true;
        }

        public String findRoute(String s) {
            String[] words = s.split(" ");
            //find nouns index
            int first_nn = -1;
            int second_nn = -1;
            for (int i = 0; i < words.length; i++) {
                if (words[i].split("/")[1].contains("NN")) {
                    if (first_nn == -1)
                        first_nn = i;
                    else
                        second_nn = i;
                }
            }
            if (!validNoun(words[first_nn].split("/")[0]) || !validNoun(words[second_nn].split("/")[0]))
                return null;

            //build graph
            Graph<String, DefaultEdge> g = buildEmptySimpleGraph();
            for (String word : words) {
                g.addVertex(word);
            }
            for (String word : words) {
                String[] word_split = word.split("/");
                int index = Integer.parseInt(word_split[word_split.length - 1]);
                if (index != 0) {
                    g.addEdge(word, words[index - 1]);
                    g.addEdge(words[index - 1], word);
                }
            }
            DijkstraShortestPath<String, DefaultEdge> dijkstraAlg = new DijkstraShortestPath<>(g);
            ShortestPathAlgorithm.SingleSourcePaths<String, DefaultEdge> iPaths = dijkstraAlg.getPaths(words[first_nn]);
            String original_path = (iPaths.getPath(words[second_nn]) + "");
            if (original_path.equals("null"))
                return null;
            String path = original_path.substring(2, original_path.length() - 1);
            path = path.replace('(', ' ');
            path = path.replace(')', ' ');
            path = path.replace(':', ' ');
            path = path.replace(',', ' ');
            String[] path_arr = path.split("\\s+");

            String output_path = "X/";
            String first_noun = "aaaaaa";
            String second_noun = "bbbbbb";
            for (int i = 0; i < path_arr.length - 1; i++) {

                if (path_arr[i].equals(path_arr[i + 1]))
                    continue;
                String[] word_arr = path_arr[i].split("/");
                if (word_arr.length == 4) {
                    if (i == 0) {
                        output_path += (word_arr[1] + "/" + word_arr[2]);
                        first_noun = word_arr[0];
                    } else
                        output_path += ("/" + word_arr[1] + "/" + word_arr[2]);
                }
            }
            String[] word_arr = path_arr[path_arr.length - 1].split("/");
            if (word_arr[0].contains(" ") || word_arr[0].equals("") || word_arr[0].contains("\t"))
                second_noun = "cccccc";
            else
                second_noun = word_arr[0];
            output_path += ("/" + word_arr[1] + "/" + word_arr[2] + "/Y");
            return output_path + " " + first_noun + " " + second_noun;
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String newval = "";
            boolean skip = false;
            newKey.set(String.valueOf(key.get()));
            String[] spt = value.toString().split("\t");
            String[] words = spt[1].split(" ");
            int noun_counter = 0;
            for (String word : words) {
                if (word.split("/").length != 4)
                    skip = true;
            }
            if (!skip) {
                for (String word : words) {
                    String[] word_data = word.split("/");
                    if (word_data[1].contains("NN")) {
                        //STEMMER
                        stemmer.setCurrent(word_data[0]);
                        stemmer.stem();
                        String new_noun = stemmer.getCurrent();
                        String newWord = word.substring(word_data[0].length());
                        newWord = new_noun + newWord;
                        noun_counter++;
                        word = newWord;
                    }
                    newval = newval + " " + word;
                }
                newval = newval.substring(1);
                if (noun_counter == 2) {
                    String route = findRoute(newval);
                    if (route != null) {
                        String[] route_arr = route.split(" ");
                        if (Integer.parseInt(spt[2]) >= dpMin) {
                            newKey.set(route_arr[1] + "," + route_arr[2] + " " + route_arr[0]); // noun1,noun2 route
                            newVal.set(spt[2]);                                                // amount
                            context.write(newKey, newVal); //< noun1,noun2 route | amount >

                        }
                    }
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        Text newKey = new Text();
        Text newVal = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
                for (Text val : values) {
                    int counter = Integer.parseInt(val.toString());
                    sum = sum + counter;
                }
                String[] spt = key.toString().split(" ");
                newKey.set(spt[0]); // noun1,noun2
                String val = spt[1] + " " + sum; // spr[1] = amount
                newVal.set(val);
                context.write(newKey, newVal);  //< noun1,noun2 | route  amount >


        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //JOB-1
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
        //job.setCombinerClass(Step1.CombinerClass.class);
        job.setReducerClass(Step1.ReducerClass.class);


        //map output <key,value>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //should be args[1]
        job.waitForCompletion(true);

    }


}