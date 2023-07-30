package org.example;

import weka.classifiers.Evaluation;
import weka.classifiers.Evaluation.*;
import weka.classifiers.bayes.NaiveBayes;
import weka.*;
import weka.classifiers.evaluation.Prediction;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.util.*;

public class Weka {
    public static void main(String[] args) throws Exception {
        FileReader file = new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\vectors.arff");
        BufferedReader reader = new BufferedReader(file);
        Instances data = new Instances(reader);
        data.setClassIndex(data.numAttributes() - 1);
        reader.close();

        DecisionStump classifier = new DecisionStump();
        classifier.buildClassifier(data);
        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(classifier, data, 10, new Random(1));
        System.out.println(eval.toSummaryString("\nResults\n======\n", false));
        System.out.println("Data needed:  ");
        System.out.println("F1: " + eval.fMeasure(0) + "\nprecision: " + eval.precision(0) + "\nrecall: " + eval.recall(0) + "\n\n\n");


        HashMap<Integer, String> nounsToCompare = new HashMap<>();
        int counter = 0;
        String line;
        FileReader file1 = new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\vectors-with-nouns.txt");
        BufferedReader read = new BufferedReader(file1);
        read.readLine();
        while ((line = read.readLine()) != null) {
            String[] spt = line.split(" ");
            String nouns = spt[spt.length - 1];
            nounsToCompare.put(counter, nouns);
            counter++;
        }

        ArrayList<Integer> tp = new ArrayList<>();
        ArrayList<Integer> tn = new ArrayList<>();
        ArrayList<Integer> fp = new ArrayList<>();
        ArrayList<Integer> fn = new ArrayList<>();

        ArrayList<Prediction> arr = eval.predictions();
        for (int i = 0; i < arr.size(); i++) {
            Prediction pred = arr.get(i);
            if (pred.actual() == 1.0 && pred.predicted() == 1.0) {
                tp.add(i);
            } else if (pred.actual() == 0.0 && pred.predicted() == 0.0) {
                tn.add(i);
            } else if (pred.actual() == 1.0 && pred.predicted() == 0.0) {
                fn.add(i);
            } else if (pred.actual() == 0.0 && pred.predicted() == 1.0) {
                fp.add(i);
            }
        }
        int counter1 = 0;

        System.out.println("\nTrue Positive: "+tp.size());
        for (int i = 0; i < tp.size(); i++) {
            System.out.println(nounsToCompare.get(tp.get(i)));
        }
        System.out.println("\nTrue Negative: "+tn.size());
        for (int i = 0; i < tn.size(); i+=400) {
                System.out.println(nounsToCompare.get(tn.get(i)));
        }
        System.out.println("\nFalse Positive: "+fp.size());
        for (int i = 0; i < fp.size(); i++) {
            System.out.println(nounsToCompare.get(fp.get(i)));
        }
        System.out.println("\nFalse Negative: "+fn.size());
        for (int i = 0; i < fn.size(); i+=200) {
            System.out.println(nounsToCompare.get(fn.get(i)));
        }


    }
}
