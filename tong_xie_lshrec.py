from pyspark import SparkContext
import itertools
from collections import Counter
sc = SparkContext(appName="INF553")
inputfile= sc.textFile('input_sample.txt')

#reading input into key-value pairs
def text_to_kv(text):
    # key: users
    key=int(text.split(',')[0][1:])
    
    # value: movies (row number x)
    output_value=[]
    for x in text.split(',')[1:]:
        output_value.append(int(x))
    return (key,output_value)

# Apply the function to matrices
movieuser = inputfile.map(text_to_kv)

# minhash to create the signature matrix
# (user id, [the row number when i=1, i=2, ...])

def minhash(t):
    user_id=t[0]
    signature=[]
    for i in range(20):
        iteration=[]
        for x in t[1]:
            iteration.append((3*x + 13*i) % 100)
        signature.append(min(iteration))
    return (user_id,signature)

signature_movie = movieuser.map(minhash)

# banding function
def bandHash(x):
    candidates=[]
    comb=list(itertools.combinations(range(0,len(banding.collect())),2)) 
    #get a combination of indices
    for c in comb:
        for i in range(5):
            if x[c[0]][1][i]==x[c[1]][1][i]: #exactly the same
                candidates.append((c[0]+1,c[1]+1))
                candidates.append((c[1]+1,c[0]+1))
    return candidates

# banding
banding=signature_movie.mapValues(lambda x:[x[4*i:4*i+4] for i in range(5)])

# getting the candidate pairs and list of users
candidates=list(set(bandHash(banding.collect())))

def user_id_list(candidates):
    user_id=[]
    for i in range(len(candidates)):
        key=int(candidates[i][0])
        user_id.append(key)
    return list(set(user_id))

user_id=user_id_list(candidates)

# define intersect, union and flatten
def intersect(a, b):
    #return the intersection of two lists
    return list(set(a) & set(b))

def union(a, b):
    #return the union of two lists
    return list(set(a) | set(b))

def flatten(list):
    flat_list=[]
    for sublist in list:
        for item in sublist:
            flat_list.append(item)
    return flat_list

#choose the top 5 similar users
def return_top_5(movieuser,user_id):
    candidates_list=[]
    for key in user_id:
        similar_users=[item[1] for item in candidates if item[0] == key]
        candidates_for_one_user=[]
        for similar_user_id in similar_users:
            a=flatten(movieuser.lookup(key))
            b=flatten(movieuser.lookup(similar_user_id))
            jaccard =len(intersect(a,b))/(len(union(a,b)))
            candidates_for_one_user.append((similar_user_id,jaccard))
            candidates_for_one_user.sort(key=lambda tup: tup[1],reverse=True) 
            top_5_for_one=candidates_for_one_user if len(candidates_for_one_user)<=5 else candidates_for_one_user [:5]
        candidates_list.append(("U"+str(key),[item[0] for item in top_5_for_one]))
    return candidates_list

similar_users=return_top_5(movieuser,user_id)

#increasing order
for i in range(len(similar_users)):
    similar=similar_users[i][1]
    similar.sort(reverse=False) 
    
#choose the top 3 movies
def return_top_3(similar_users):
    movie_recommend=[]
    for i in range(len(similar_users)):
        users_list=similar_users[i][1]
        movies=[]
        for u in users_list:
            user_movies=flatten(movieuser.lookup(u))
            movies.extend(user_movies)
        movies_sorted=sorted(Counter(movies), key=Counter(movies).__getitem__, reverse=True)
        top_movies=movies_sorted if len(movies_sorted)<=3 else movies_sorted[:3]
        movie_recommend.append((similar_users[i][0],top_movies))
    return movie_recommend

return_top_3(similar_users)                
                









