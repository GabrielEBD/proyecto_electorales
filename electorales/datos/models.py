from django.db import models

# Create your models here.

class Author(models.Model):
    name = models.CharField(max_length=100, null=True, blank=True)
    username = models.CharField(max_length=100, null=True, blank=True)
    user_id = models.CharField(max_length=50, null=True, blank=True)

class Tweet(models.Model):
    author = models.ForeignKey(Author,on_delete=models.SET_NULL,null=True, blank=True)
    id_tweet = models.CharField(max_length=100, null=True, blank=True)
    full_text = models.TextField(null=True, blank=True)
    created_at = models.CharField(max_length=100, null=True, blank=True)
    retweet_count = models.IntegerField(null=True, blank=True,default=0)
    reply_count = models.IntegerField(null=True, blank=True,default=0)
    like_count = models.IntegerField(null=True, blank=True,default=0)
    quote_count = models.IntegerField(null=True, blank=True,default=0)
    lang = models.CharField(max_length=100, null=True, blank=True)








class Revisados(models.Model):
    id_revisado = models.CharField(max_length=100, null=True, blank=True)
