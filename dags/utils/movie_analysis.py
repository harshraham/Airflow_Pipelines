import pandas as pd
from sklearn.metrics.pairwise import nan_euclidean_distances
import numpy as np

ratings_df=pd.read_csv('/opt/airflow/dags/data/ml-100k/u.data',sep='\t',header=None,names=['user_id','item_id','rating','timestamp'])
item_df=pd.read_csv('/opt/airflow/dags/data/ml-100k/u.item',sep='|',header=None,names=['movie_id' , 'movie_title' , 'release_date' , 'video_release_date' ,
                                                                'IMDb_URL' , 'unknown' , 'Action' , 'Adventure' , 'Animation' ,
                                                                'Childrens' , 'Comedy' , 'Crime' , 'Documentary' , 'Drama' , 'Fantasy' ,
                                                                'Film-Noir' , 'Horror' , 'Musical' , 'Mystery' , 'Romance' , 'Sci-Fi' ,
                                                                'Thriller' , 'War' , 'Western'],encoding='latin-1')
occupation_df=pd.read_csv('/opt/airflow/dags/data/ml-100k/u.occupation',header=None,names=['occupation'])
user_df=pd.read_csv('/opt/airflow/dags/data/ml-100k/u.user',sep='|',header=None,names=['user_id' , 'age' , 'gender' , 'occupation' , 'zip_code'])
genre_df=pd.read_csv('/opt/airflow/dags/data/ml-100k/u.genre',sep='|',header=None,names=['genre' , 'genre_id'])

#Find the mean age of users in each occupation
def task1():
    print(user_df.groupby('occupation')[['age']].mean('age'))
    return user_df.groupby('occupation')[['age']].mean('age')

#Find the names of top 20 highest rated movies. (at least 35 times rated by Users)
def task2():
    out=ratings_df.merge(item_df,left_on='item_id',right_on='movie_id')[['movie_title','user_id','rating']] \
        .groupby('movie_title') \
        .agg({'user_id':'count', 'rating':'mean'}) \
        .rename(columns={'user_id':'rating_count','rating':'mean_rating'}) \
        .query('rating_count>=35') \
        .sort_values('mean_rating',ascending=False).head(20)
    print(out)
    return out

#Find the top genres rated by users of each occupation in every age-groups. age-groups can be defined as 20-25, 25-35, 35-45, 45 and older
def task3():
    bins= [20,25,30,35,45,100]
    labels = ['20-25','25-30','30-35','35-45','45 and older']
    user=user_df.copy()
    user['age_group']=pd.cut(user['age'],bins=bins, labels=labels, right=False)
    joined_df=user.merge(ratings_df,on='user_id').merge(item_df,left_on='item_id',right_on='movie_id')[['user_id','occupation','age_group','rating','movie_title','unknown' , 'Action' , 'Adventure' , 'Animation' ,
                                                                                                        'Childrens' , 'Comedy' , 'Crime' , 'Documentary' , 'Drama' , 'Fantasy' ,
                                                                                                        'Film-Noir' , 'Horror' , 'Musical' , 'Mystery' , 'Romance' , 'Sci-Fi' ,
                                                                                                        'Thriller' , 'War' , 'Western']]
    grouped_df= joined_df.groupby(['occupation','age_group'])[['unknown' , 'Action' , 'Adventure' , 'Animation' ,
                                                               'Childrens' , 'Comedy' , 'Crime' , 'Documentary' , 'Drama' , 'Fantasy' ,
                                                               'Film-Noir' , 'Horror' , 'Musical' , 'Mystery' , 'Romance' , 'Sci-Fi' ,
                                                               'Thriller' , 'War' , 'Western']].sum()
    grouped_df['top_rated_genre']=grouped_df.idxmax(axis=1)
    print(grouped_df[['top_rated_genre']])
    return grouped_df[['top_rated_genre']]

#given a movie, find top 10 similar movie based on user ratings
def prepare_similarity_cooccurence_data():
    ratings_df['rating_norm']=(ratings_df['rating']-1)/4 #min-max normalization

    #co occurance
    ratings_df['occurence']=1
    user_movie_matrix=ratings_df.pivot(index='user_id',columns=['item_id'],values='occurence').fillna(0)
    co_occurence_matrix=np.dot(user_movie_matrix.T,user_movie_matrix)
    co_occurence_df=pd.DataFrame(co_occurence_matrix,index=user_movie_matrix.columns,columns=user_movie_matrix.columns)

    #co-occurence threshold=50
    valid_rating_transform=co_occurence_df.applymap(lambda x:0 if x<50 else 1)

    #using nan_euclidean_distance
    #dist(x,y) = sqrt(weight * sq. distance from present coordinates) where, weight = Total # of coordinates / # of present coordinates
    pivot_df=ratings_df.pivot(index='item_id',columns=['user_id'],values=['rating_norm'])
    nan_euclid_dist=pd.DataFrame(nan_euclidean_distances(pivot_df),columns=[i for i in range(1,len(pivot_df)+1)],index=[i for i in range(1,len(pivot_df)+1)])

    #similarity(x,y)=1/(1+dist(x,y)
    nan_euclid_similarity=nan_euclid_dist.applymap(lambda x:1/(1+x))

    #valid ones are those with co occurence>=50
    valid_nan_euclid_similarity=nan_euclid_similarity*valid_rating_transform
    return valid_nan_euclid_similarity,co_occurence_df

def find_simialar_movies(title):
    valid_nan_euclid_similarity,co_occurence_df=prepare_similarity_cooccurence_data()
    movie_id=int(item_df[item_df['movie_title']==title].movie_id.iloc[0])
    similar_movies=valid_nan_euclid_similarity[movie_id].sort_values(ascending=False)
    similar_movies=pd.DataFrame(similar_movies[similar_movies.index!=movie_id]).reset_index()[:10]
    cooccurences=pd.DataFrame(co_occurence_df[movie_id]).reset_index().rename(columns={movie_id:'strength'})
    similar_movies=similar_movies.merge(item_df, left_on='index',right_on='movie_id')[['movie_id','movie_title',movie_id]] \
        .rename(columns={movie_id:'score'}) \
        .merge(cooccurences,left_on='movie_id',right_on='item_id')[['movie_title','score','strength']]
    return similar_movies

def task4():
    out=find_simialar_movies('Usual Suspects, The (1995)')
    print(out)
    return out