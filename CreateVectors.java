package org.example;

import org.tartarus.snowball.ext.englishStemmer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class CreateVectors {

    public static void main(String[] args) throws IOException {

        // create HashMap fot routes
        BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\src\\main\\java\\org\\example\\input-dpMin-30.txt"));
        Map<String, Integer> routes = new HashMap<>();
        int counter = 0;
        String line;
        int pairCounter = 0;
        int routeCounter = 0;


        while ((line = reader.readLine()) != null) {
            line = line.replace('-', ' ');
            String[] spt = line.split("\\s+");
            pairCounter++;
            for (int i = 1; i < spt.length; i = i + 2) {
                if (!routes.containsKey(spt[i])) {
                    routes.put(spt[i], counter);
                    counter++;
                    routeCounter++;

                }
            }
        }
        System.out.println("there are: " + pairCounter + " diffrent nouns pairs");
        System.out.println("there are: " + routeCounter + " diffrent routes");

        reader.close();


        //run stemmer on hypernym.txt file
        BufferedReader reader1 = new BufferedReader(new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\src\\main\\java\\org\\example\\hypernym.txt"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("stemmed-hypernym.txt"));
        int trueCounter = 0;
        int falseCounter = 0;
        String line1;
        while ((line1 = reader1.readLine()) != null) {
            String[] spt1 = line1.split("\\s+");
            englishStemmer stemmer = new englishStemmer();

            stemmer.setCurrent(spt1[0]);
            stemmer.stem();
            String new_noun1 = stemmer.getCurrent();

            stemmer.setCurrent(spt1[1]);
            stemmer.stem();
            String new_noun2 = stemmer.getCurrent();

            writer.write(new_noun1 + " " + new_noun2 + " " + spt1[2]);
            if (spt1[2].equals("True"))
                trueCounter++;
            else
                falseCounter++;

            writer.newLine();
        }
        System.out.println("num of true: " + trueCounter);
        System.out.println("num of false: " + falseCounter);

        reader1.close();
        writer.close();


        //create vectors file
        BufferedWriter writer1 = new BufferedWriter(new FileWriter("vectors.txt"));

        BufferedReader reader2 = new BufferedReader(new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\src\\main\\java\\org\\example\\input-dpMin-30.txt"));

        BufferedReader reader3 = new BufferedReader(new FileReader("C:\\Users\\netx1\\Desktop\\computer science\\mevuzarot\\ass3\\hypernym-detection\\src\\main\\java\\org\\example\\stemmed-hypernym.txt"));
        String line3;
        HashMap<String, String> hypernymList = new HashMap<>();
        while ((line3 = reader3.readLine()) != null) {
            String[] spt = line3.split(" ");
            String key = spt[0] + "," + spt[1];
            hypernymList.put(key, spt[2]); //< noun1.noun2 | True/False >
        }
        String f = "feature0";
        for (int i = 1; i < routes.size(); i++) {
            f = f + " " + "feature" + i;
        }
        f = f + " " + "class";

        String line2;
        Object[] newVector = new Object[routes.size() + 1];
        int[] zeroes = new int[routes.size()];
        writer1.write(f); //add features vector
        writer1.newLine();
        int hypernymCounter = 0;
        while ((line2 = reader2.readLine()) != null) {
            for (int i = 0; i < zeroes.length; i++) {
                newVector[i] = new Integer(0);
            }

            line2 = line2.replace('-', ' ');
            String[] nounsRoute = line2.split("\\s+");
            String bool = hypernymList.get(nounsRoute[0]);
            if (bool != null) {
                hypernymCounter++;
                if (nounsRoute.length % 2 == 1) {
                    for (int i = 1; i < nounsRoute.length; i = i + 2) {
                        String route = nounsRoute[i];
                        String amount2 = nounsRoute[i + 1];
                        int indexOfRoute = routes.get(route);
                        newVector[indexOfRoute] = Integer.parseInt(amount2);

                    }
                    if (bool.equals("True"))
                        newVector[routes.size()] = "True";
                    else
                        newVector[routes.size()] = "False";

                    // write to vector file
                    String s = "";
                    for (int j = 0; j < newVector.length; j++)
                        s = s + " " + newVector[j];
                    s = s.substring(1);
                    writer1.write(s);
                    writer1.newLine();
                }
            }
        }
        writer1.close();
        reader2.close();
        reader3.close();
        System.out.println("there are: " + hypernymCounter + " word pairs that match hypernym file");

    }
}

