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
the side data generated in job 1. The reducer/combiner simply does and element-wise sum of all
the maps for a given word/key giving (word1, {word2:sum, word3:sum,...}). The reducer then loops
through all the keys of the map of a given word, and does the same operation described in pairs
except the P(word1,word2) is obtained from the element-wise sum map. P(word1) and P(word2) are
obtained from the side data. It is then output as ((word1, word2), PMI).

Question 2:
-----------

Question 3:
-----------

Question 4:
-----------

Question 5:
-----------

Question 6:
-----------

Question 7:
-----------
