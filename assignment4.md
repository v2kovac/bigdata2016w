Works as far as I can see, haven't tested altiscale but it should work as well.  

# Assignment 4 Marks

| SL  |  ML | SA | SA |
| --- | --- | --- | --- |
|15|5|10|0|


<!--* Penalty: %-->
* Total: 30/55


* SL (15 points)
 * A correct implementation of single-source personalized PageRank is worth 10 points.
 * That we are able to run the single-source personalized PageRank implementation in the Linux Student CS environment is worth 5 points.
* ML (20 points)
 * A correct implementation of multiple-source personalized PageRank is worth 15 points.
 * That we are able to run the multiple-source personalized PageRank implementation in the Linux Student CS environment is worth 5 points.
* SA (10 points)
 * Scaling the single-source personalized PageRank implementation on Altiscale is worth 10 points.
* MA (10 points)
 * Scaling the multiple-source personalized PageRank implementation on Altiscale is worth 10 points.



## Deducted Points Detail

output mismatched @both machines

e.g., output from altiscale 

```
Source: 4921							Source: 4921
0.15399 4921						      |	0.00000 4468404
0.00273 2939323						      |	0.00000 4446434
0.00258 11148971					      |	0.00000 46632907
0.00214 3434750						      |	0.00000 35064364
0.00199 268111						      |	0.00000 15040841
0.00189 235003						      |	0.00000 21670358
0.00188 23618380					      |	NaN 316
0.00185 108956						      |	NaN 336
0.00184 9428						      |	NaN 586
0.00178 38301						      |	0.00000 48699969
```

unable to check output @altiscale multisource

