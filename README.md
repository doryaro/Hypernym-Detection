# Hypernym-Detection
## Tools
Google books Syntactic N-grams Biarcs \
stemmer \
Weka: Machine Learning Algorithm

## how it works
Example of the input: Google Books Syntactic N-grams Biarcs can be seen in the file: \
input-dpMin-30.txt
### step1
The input is a file of the Biarcs. \
Run a stemmer on the pairs of nouns and find the route between any two nouns. \
the output looks like this: \
<noun1 , noun2 | route amount>
### step2
Combine all the different routes for a pair of nouns \
The output looks like this: \
< noun1, noun2 | route1 amount1 route2 amount2 > 

#### Now we get back a file in which every line has a pair of words and all their different routes

### CreateVectors class:
1. Run stemmer on the hypernym file
2. Count the number of different routes in the file recovered from reduce-map (number
The different routes will be the size of the vector)
3. Create a file of the vectors in txt format
4. With the help of a small change in the code we insert, you can also get a with-vectors-nouns.txt file

Then when we have the vectors.txt file in hand we convert it to a .csv file with the help of excel \
Afterward convert it to an .arff file with the help of weka \
### Weka class:
1. Run the classifier DecisionStump using the Weka library with Weka 10-fold cross-validation
2. Find the values of Precision and Recall, F1







