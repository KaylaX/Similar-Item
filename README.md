# Similar-Item
write a Spark Python program which uses LSH to efficiently find similar users and make recommendations


Finding similar users: Apply minhash to obtain a signature of 20 values for each user. Recall that this is done by permuting the rows of characteristic matrix of movie-user matrix (i.e., row are movies and columns represent users).

Assume that the i-th hash function for the signature: h(x,i) = (3x + 13i) % 100, where x is the original row number in the matrix. Note that i ranges from 0 to 19.

Apply LSH to speed up the process of finding similar users, where the signature is divided into 5 bands, with 4 values in each band.

Making recommendations: Based on the LSH result, for each user U, find top-5 users who are most similar to U (by their Jaccard similarity, if same, choose the user that has smallest ID), and recommend top-3 movies to U where the movies are ordered by the number of these top-5 users who have watched them (if same, choose the movie that has the smallest ID).

