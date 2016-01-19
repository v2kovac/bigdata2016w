Question 1:
-----------
Pairs:
This is 2 MapReduce jobs. The Mapper of the first job goes through every line and emits
every unique word once as (word, 1), it also keeps count of how many lines there are.
In the Reducer, I sum it up and emit (word, sum) to an output file.
The Mapper of the second job takes Shakespeare as the input and emits every word pair,
except the self pair, like so ((word1, word2), 1) using Jimmy's PairsOfStrings.
The Reducer then does a setup where it gets the total number of lines from the first job, then
reads the data from the output file and stores it as a hashmap. Then the combiner/reducer sums
up every pair ((word1, word2), sum) then the reducer calculates the PMI by using that
sum / totalLines, divided by the (word1, sum)/totalLines multiplied by
(word2, sum) / totalLines, all log base 10 where word1 sum and word2 sum is obtained by the
side data hashmap. It is output as ((word1, word2), PMI) in the specified ouptut file.

Stripes:
This is 2 MapReduce jobs. First job is exactly the same as Pairs.
The Mapper of the second job emits every word with a map of the rest of the words (except itself)
in a hashmap like so (word1, {word2:1, word3:1,..}). The reducer does the setup again
where it gets the total lines from job 1, and a hashmap of the frequency of every word from
the side data generated in job 1. The reducer/combiner simply does an element-wise sum of all
the maps for a given word/key giving (word1, {word2:sum, word3:sum,...}). The reducer then loops
through all the keys of the map of a given word, and does the same operation described in pairs
except the P(word1,word2) is obtained from the element-wise sum map. P(word1) and P(word2) are
obtained from the side data. It is then output as ((word1, word2), PMI).

Question 2:
-----------
Running on linux.student.cs.uwaterloo.ca  
Pairs: Job1 6.062 + Job2 49.786 = 55.848 seconds  
Stripes: Job1 6.098 + Job2 14.742 = 20.840 seconds  

Question 3:
-----------
Running on linux.student.cs.uwaterloo.ca  
Pairs: Job1 8.174 + Job2 60.773 = 68.947 seconds  
Stripes: Job1 9.217 + Job2 16.752 = 25.969 seconds  

Question 4:
-----------
77198 lines(ie. pairs) according to the python script provided  
This is double the amount of unique pairs since I emit all pairs
therefore around 38599 unique pairs.

Question 5:
-----------
(maine, anjou) with PMI = 3.6331423021951013  
Margaret of Anjou is a major character in a tetrology Shakespeare wrote.  
The dowry she was attached to were the lands of Anjou and Maine therefore we would  
expect these words to appear often in the same line, and hardly ever on their own outside of  
that set of stories which gives a high PMI.  

Question 6:
-----------
(tears, shed)       2.1117900768762365  
(tears, salt)       2.0528122169168985  
(tears, eyes)       1.1651669643071034  

(death, father's)   1.1202520304197314  
(death, die)        0.7541593889996885  
(death, life)       0.7381345918721788  


Question 7:
-----------
(waterloo, kitchener)   2.614997334922472  
(waterloo, napoleon)    1.9084397672287545  
(waterloo, napoleonic)  1.786618971523  

(toronto, marlboros)    2.353996461343968  
(toronto, spadina)      2.3126037761857425  
(toronto, leafs)        2.3108905807416225  
