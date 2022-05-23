from django.shortcuts import render
import tweepy
import datetime
import time
import pandas as pd
import pymongo
from datos.models import * 

#clase para el control de la info con mongoDB
class MongoFullDataObject():

    variables = ['quote_count','like_count','reply_count','retweet_count','created_at','name','username','user_id','id_tweet','full_text','lang']
    json_object = {}

    def __init__(self, *args, **kwargs):
        for variable in self.variables:
            self.json_object[variable] = kwargs.get(variable, None)
        for key in kwargs.keys():
            if not key in self.variables:
                raise IOError("El atributo "+key+" no esta soportado")


def agrupar_tweets():
    archivos = [
        'data_19_04_2022.csv',
        'data_02_05_2022.csv',
        'data_03_05_2022.csv',
        'data_21_04_2022.csv',
        'data_22_04_2022.csv',
        'data_25_04_2022.csv',
        'data_27_04_2022.csv',
        'data_28_04_2022.csv',
    ]
    ruta = './archivos_datos/'
    dict_twitter_data = []
    for archivo in archivos:
        print (archivo)
        tweets = pd.read_csv(ruta+archivo)
        i = 0
        for fila in range(int(tweets.size/10)):
            id = tweets['id'][i]
            text = tweets['text'][i]
            dict_twitter_data.append({'id_tweet':id,'text':text})
            i+=1
    data_frame = pd.DataFrame(dict_twitter_data)
    print(data_frame.size)
    data_frame.to_csv('total_data.csv', encoding='utf-8')

#Clase para obtener data de los tweets por stream
class CustomStream(tweepy.StreamingClient):

    def __init__(self, *args, **kwargs):
        super(CustomStream, self).__init__(*args, **kwargs)
        self.dict_twitter_data = []

    def on_tweet(self, tweet):
        self.dict_twitter_data.append({
            'id': tweet.id,
            'author_id': tweet.author_id,
            'text':tweet.text.replace('\n', ' ').replace('\r', ''),
            'context_annotations':tweet.context_annotations,
            'conversation_id':tweet.conversation_id,
            'created_at':tweet.created_at,
            'geo':tweet.geo,
            'possibly_sensitive':tweet.possibly_sensitive,
            'source':tweet.source
        })
        if len(self.dict_twitter_data)%1000==0:
            print(len(self.dict_twitter_data))
        if len(self.dict_twitter_data) >= 60000:

            data_frame = pd.DataFrame(self.dict_twitter_data)
            time = datetime.date.today().strftime('%d_%m_%Y')
            data_frame.to_csv('data_'+time+'.csv',encoding='utf-8')
            self.disconnect()

#aplicacion sensilla de la clase 
def recolectar_datos():
    stream = CustomStream("AAAAAAAAAAAAAAAAAAAAAJLRbQEAAAAAA2AWS3eie2QqJ%2BNS%2Bgvb3ilSB%2B8%3DqbVfospXUBhOouO2nDaL1IhJkSkojlUxMMVRmcGHJoJRBPQIs4")

    search_words =[
        'lang:es bio_location:Colombia OR place:Colombia OR place_country:CO petro OR fico OR #FicoPresidente OR #PetroPresidente OR #ficopresidente OR #petropresidente',

    ]
    reglas = []
    for string in search_words:
        reglas.append(tweepy.StreamRule(string))

    #stream.sample()
    stream.add_rules(reglas)
    stream.filter()

def test_conexion_mongo():
    client = pymongo.MongoClient("localhost:27017")
    db = client.proyecto_tweets
    tweets = db.tweets
    new_tweets = [
        {"id":1,"text":"hola"},
        {"id": 2, "text": "hola"},
        {"id": 3, "text": "hola"},
    ]
    one  = {"id": 3, "text": "hola"}
    tweets.insert_one(one)

#Recolreccion de la info por tweet (informacion especifica) guadato a Postgres db y mongodb
def get_info_tweet_by_id()  :


    archivos = [
        'data_19_04_2022.csv',
        'data_02_05_2022.csv',
        'data_03_05_2022.csv',
        'data_21_04_2022.csv',
        'data_22_04_2022.csv',
        'data_25_04_2022.csv',
        'data_27_04_2022.csv',
        'data_28_04_2022.csv',
    ]
    ruta = './archivos_datos/'
    
    lista_usuarios_sql = []
    lista_tweets_sql = []
    lista_tweets_mongodb = []

    dict_user_model = {}
    dict_tweet_id = {}
    
    cliente = tweepy.Client("AAAAAAAAAAAAAAAAAAAAAJLRbQEAAAAAA2AWS3eie2QqJ%2BNS%2Bgvb3ilSB%2B8%3DqbVfospXUBhOouO2nDaL1IhJkSkojlUxMMVRmcGHJoJRBPQIs4")
    i = 0 
    revisados = list(set(Revisados.objects.all().values_list('id_revisado',flat=True)))
    nuevos_revisados = []
    for archivo in archivos:
        tweets_df = pd.read_csv(ruta+archivo)
        for fila in range(int(tweets_df.size/10)):
            if not str(tweets_df['id'][fila]) in revisados:
                
                time.sleep(3)
                nuevos_revisados.append(str(tweets_df['id'][fila]))
                try:
                    tweet = cliente.get_tweet(
                        id=tweets_df['id'][fila],
                        tweet_fields=['created_at','geo','lang','public_metrics','author_id'],
                        user_fields=['username','created_at','location','public_metrics'],
                        expansions=['author_id'])
                except Exception as e:
                    print('-- reinicio de sesion --')
                    time.sleep(5)

                    cliente = tweepy.Client("AAAAAAAAAAAAAAAAAAAAAJLRbQEAAAAAA2AWS3eie2QqJ%2BNS%2Bgvb3ilSB%2B8%3DqbVfospXUBhOouO2nDaL1IhJkSkojlUxMMVRmcGHJoJRBPQIs4")
                    tweet = cliente.get_tweet(
                        id=tweets_df['id'][fila],
                        tweet_fields=['created_at','geo','lang','public_metrics','author_id'],
                        user_fields=['username','created_at','location','public_metrics'],
                        expansions=['author_id']
                        )
                if tweet.data:
                    dict_tweet_completo = {
                        'quote_count':tweet.data.data['public_metrics']['quote_count'],
                        'like_count':tweet.data.data['public_metrics']['like_count'],
                        'reply_count':tweet.data.data['public_metrics']['reply_count'],
                        'retweet_count':tweet.data.data['public_metrics']['retweet_count'],
                        'created_at':tweet.data.data['created_at'],
                        'name':tweet.includes['users'][0].name,
                        'username':tweet.includes['users'][0].username,
                        'user_id':tweet.data.data['author_id'],
                        'id_tweet':tweet.data.data['id'],
                        'full_text':tweet.data.data['text'],
                        'lang':tweet.data.data['lang']
                    }
                    lista_tweets_mongodb.append(MongoFullDataObject(**dict_tweet_completo).json_object)
                    

                    autor = Author(**{
                        'user_id':tweet.data.data['author_id'],
                        'username':tweet.includes['users'][0].username,
                        'name':tweet.includes['users'][0].name
                    })
                    dict_user_model[tweet.data.data['author_id']] = autor
                    lista_usuarios_sql.append(autor)

                    dict_tweet_id[tweet.data.data['id']]={
                        'quote_count':tweet.data.data['public_metrics']['quote_count'],
                        'like_count':tweet.data.data['public_metrics']['like_count'],
                        'reply_count':tweet.data.data['public_metrics']['reply_count'],
                        'retweet_count':tweet.data.data['public_metrics']['retweet_count'],
                        'created_at':tweet.data.data['created_at'],
                        'author':tweet.data.data['author_id'],
                        'id_tweet':tweet.data.data['id'],
                        'full_text':tweet.data.data['text'],
                        'lang':tweet.data.data['lang']
                    }
                    
                    #SISTEMA DE GUARDADO CADA 10.000 TWEETS PARA EVITAR PROBLEMAS DE PERDIDA DE INFORMACION EN CASOS DE ERRORES EN EL PC LOCAL 
                    if i % 1000 == 0 and i != 0:
                        #conexion con mongoDB local (MongoDB Compass Windows 10)
                        client = pymongo.MongoClient("localhost:27017")
                        db = client.proyecto_tweets
                        tweets = db.tweets

                        #GUARDADO DE DATOS EN MONGO
                        tweets.insert_many(lista_tweets_mongodb)
                        lista_tweets_mongodb = []

                        #GUARDADO DE AUTORES EN POSTGRES
                        Author.objects.bulk_create(lista_usuarios_sql)
                        lista_usuarios_sql = []

                        #ASIGNACION DE ForeignKey DE AUTOR A CADA TWEET Y CREACION DE TWEETS 
                        for id_tweet in dict_tweet_id:
                            tweet = dict_tweet_id[id_tweet]
                            tweet['author'] = dict_user_model[tweet_dict['author']]
                            lista_tweets_sql.append(Tweet(**tweet))
                        Tweet.objects.bulk_create(lista_tweets_sql)
                        lista_tweets_sql = []

                        #ACTUALIZACION DE TWEETS YA REVISADOS EN CASO DE CORTE O PAUSA DEL SCRIPT
                        lista_revisados_nuevos = []
                        for id_nuevo in nuevos_revisados:
                            lista_revisados_nuevos.append(Revisados(**{'id_revisado':id_nuevo}))
                        Revisados.objects.bulk_create(lista_revisados_nuevos)
                        nuevos_revisados=[]
                        revisados = list(set(Revisados.objects.all().values_list('id_revisado',flat=True)))
                        print(i)

                    i+=1

    #PROCESO DE GUARDADO DE TWEETS FALTANTES
    Author.objects.bulk_create(lista_usuarios_sql)

    for id_tweet in dict_tweet_id:
        tweet = dict_tweet_id[id_tweet]
        tweet['author'] = dict_user_model[tweet_dict['author']]
        lista_tweets_sql.append(Tweet(**tweet))
    Tweet.objects.bulk_create(lista_tweets_sql)

    #conexion con mongoDB local (MongoDB Compass Windows 10)
    client = pymongo.MongoClient("localhost:27017")
    db = client.proyecto_tweets
    tweets = db.tweets
    tweets.insert_many(lista_tweets_mongodb)
    
