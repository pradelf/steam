# Databricks notebook source
# MAGIC %md
# MAGIC # La plateforme de jeux video Steam  👾
# MAGIC 960 min
# MAGIC
# MAGIC ci-dessous, vous trouvez le lien vers la publication publique du rendu sur DataBricks en version Community : 
# MAGIC
# MAGIC [https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3196898497095552/1371440223075587/2122917452595674/latest.html](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3196898497095552/1371440223075587/2122917452595674/latest.html)
# MAGIC
# MAGIC ## Description de la société 📇
# MAGIC
# MAGIC
# MAGIC Steam est un service de distribution numérique de jeux vidéo et une boutique en ligne créée par Valve. Il a été lancé en septembre 2003 sous la forme d’un client logiciel destiné à fournir automatiquement les mises à jour pour les jeux de Valve, avant de s’étendre à la distribution de titres tiers à la fin de l’année 2005.
# MAGIC
# MAGIC Steam propose diverses fonctionnalités, comme la gestion des droits numériques (DRM), l’association de joueurs sur des serveurs de jeu avec le système anti-triche de Valve (Valve Anti-Cheat), le réseau social intégré, ainsi que des services de streaming de jeux.
# MAGIC
# MAGIC Le client Steam offre notamment l’automatisation des mises à jour, le stockage dans le cloud pour les sauvegardes, ainsi que des fonctions communautaires telles que la messagerie directe, l’overlay en jeu et un marché virtuel de collectibles.
# MAGIC
# MAGIC
# MAGIC ## Projet 🚧
# MAGIC
# MAGIC You're working for Ubisoft, a French video game publisher. They'd like to release a new revolutionary videogame! They asked you conduct a global analysis of the games available on Steam's marketplace in order to better understand the videogames ecosystem and today's trends.
# MAGIC
# MAGIC ## Objectifs 🎯
# MAGIC
# MAGIC Tu travailles pour Ubisoft, un éditeur français de jeux vidéo. Ils souhaitent sortir un nouveau jeu révolutionnaire ! Ils t’ont demandé de mener une analyse globale des jeux disponibles sur la marketplace de Steam afin de mieux comprendre l’écosystème vidéoludique et les tendances actuelles.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Portée du projet 🖼️
# MAGIC Tu devras utiliser Databricks et PySpark pour réaliser cette analyse exploratoire des données (EDA). En particulier, tu devras utiliser l’outil de visualisation de Databricks pour créer les graphiques.
# MAGIC Le jeu de données est disponible dans notre bucket S3 à l’URL suivante : [s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json](s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json).
# MAGIC
# MAGIC ## Aides 🦮
# MAGIC
# MAGIC Pour t’aider à mener à bien ce projet, voici quelques conseils qui devraient t’être utiles :
# MAGIC Pour adopter différents niveaux d’analyse, il peut être pertinent de créer plusieurs dataframes.
# MAGIC Étant donné que le jeu de données est semi-structuré avec un schéma imbriqué, les méthodes PySpark comme getField() et explode() peuvent t’aider.
# MAGIC Le jeu de données contient des champs texte et des champs de dates : PySpark propose des fonctions utilitaires pour manipuler efficacement ces types de données 💡
# MAGIC Tu peux utiliser des fonctions d’agrégation et groupBy pour réaliser des analyses segmentées.
# MAGIC
# MAGIC ## Rendu 📬
# MAGIC
# MAGIC Pour mener ce projet à son terme, tu devras livrer :
# MAGIC Un ou plusieurs notebooks comprenant la manipulation des données avec PySpark et la visualisation des données avec l’outil de tableau de bord de Databricks.
# MAGIC Pour t’assurer que le jury puisse consulter toutes les visualisations, utilise le bouton « publish » dans les notebooks Databricks afin de créer une URL publique où une copie de ton notebook sera accessible.
# MAGIC Lors de l’utilisation du bouton « publish », Databricks peut t’indiquer que la taille de ton notebook dépasse la limite autorisée. Si cela arrive, divise simplement ton travail en plusieurs notebooks.
# MAGIC Merci de copier-coller le ou les liens vers tes notebooks publiés dans ton dépôt GitHub afin que le jury puisse y accéder facilement. 😌
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importation des librairies de Spark et PySpark
# MAGIC

# COMMAND ----------

spark

sc = spark.sparkContext

from pyspark.sql.types import *


from pyspark.sql.types import * # Import types to convert columns using spark sql
from pyspark.sql import functions as F # This will load the class where spark sql functions are contained
from pyspark.sql import Row # this will let us manipulate rows with spark sql

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecture des données et petite analyse exploratoire.

# COMMAND ----------

# Bucket d'input : s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json
steam_path = "s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json"

df = spark.read.json(steam_path)

df.select('data.release_date').show(5)
print("Price")
display(df.select('data.price').show(5))
print("ccu")
display(df.select('data.ccu').show(25))
print("discount")
display(df.select('data.discount').show(25))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Le fichier est assez compact sur deux colonnes et va nécessiter de ventiler (explode) le Dataste pour le rendre plus lisible.

# COMMAND ----------

df.schema.jsonValue()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField
from typing import List, Dict, Generator, Union, Callable

# This is actually written like a scala function, we'll walk you through it
def walkSchema(schema: Union[StructType, StructField]) -> Generator[str, None, None]:
    """Explores a PySpark schema:
    
    schema: StructType | StructField
    
    Yield
    -----
    A generator of strings, the name of each field in the schema
    """
    
    # we define a function _walk that produces a string generator from
    # a dictionnary "schema_dct", and a string "prefix"
    def _walk(schema_dct: Dict['str', Union['str', list, dict]],
              prefix: str = "") -> Generator[str, None, None]:
        assert isinstance(prefix, str), "prefix should be a string" # check if prefix is a string
        
        # this function returns "name" if there's no prefix and "prefix.name" if prefix exists
        fullName: Callable[str, str] = lambda name: ( 
            name if not prefix else f"{prefix}.{name}")
        
        # we get the next name one level lower from the dictionnary
        name = schema_dct.get('name', '')
        
        # if the type is struct then we search for the fields key
        # if fields is there we apply the function again and dig one level deeper in
        # the schema and set a prefix
        if schema_dct['type'] == 'struct':
            assert 'fields' in schema_dct, (
                "It's a StructType, we should have some fields")
            for field in schema_dct['fields']:
                yield from _walk(field, prefix=prefix)
        # if we have a dict type and we can't find fields then we
        # dig one level deeper and apply the _walk function again
        elif isinstance(schema_dct['type'], dict):
            assert 'fields' not in schema_dct, (
                "We're missing some keys here")
            yield from _walk(schema_dct['type'], prefix=fullName(name))
        # If we finally reached the end and found a name we yield the full name
        elif name:
            yield fullName(name)
    
    yield from _walk(schema.jsonValue())

# yield as opposed to return, returns a result but does not stop the function from running, it keeps
# running even after returning one result.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ventilation des catégories.

# COMMAND ----------

# MAGIC %md
# MAGIC Nombre de jeux

# COMMAND ----------

games_nb = df.select(F.col('id')).distinct().count()
print(" il y a ",games_nb, " sur la plateforme Steam.")

# COMMAND ----------

categorie_nb = df.select(F.explode('data.categories')).count()

# COMMAND ----------

display(categorie_nb)

# COMMAND ----------

categorie_df = df.select(F.explode('data.categories')).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Notre jeu de données contient 191270 catégories réparties sur 20 catégories uniques de jeu.

# COMMAND ----------

genre_nb = df.select(F.col('data.genre')).distinct().count()
print(genre_nb)

# COMMAND ----------

# MAGIC %md
# MAGIC Cela révèle une grande quantité de jeux proposéq sur la plateforme Steam couvrant un grand nombre de catégories..

# COMMAND ----------

genre_df = df.select(F.col('data.genre')).distinct().show(10)

# COMMAND ----------

games_df=df.withColumn('id', F.col('id')) \
          .withColumn('app_id', F.col('data.appid')) \
          .withColumn('name', F.col('data.name')) \
          .withColumn('genre', F.col('data.genre')) \
          .withColumn('price', F.col('data.price')) \
          .withColumn('publisher', F.col('data.publisher')) \
          .withColumn('type', F.col('data.type')) \
          .withColumn('required_age', F.col('data.required_age'))  \
          .withColumn('positive', F.col('data.positive'))  \
          .withColumn('negative', F.col('data.negative'))  \
          .withColumn('languages', F.col('data.languages'))  \
          .withColumn('discount', F.col('data.discount')) \
          .withColumn('release_date', F.to_timestamp(F.col("data.release_date"), format="y/M/d")) \
          .drop('data')
game_categories_df=df.withColumn('app_id', F.col('data.appid')) \
                     .withColumn('categories', F.explode('data.categories'))
games_df.select('name').orderBy(F.desc("positive")).show(15)
games_platform_df= df.withColumn('id', F.col('id')) \
                     .withColumn('linux', F.col('data.platforms.linux')) \
                     .withColumn('mac', F.col('data.platforms.mac')) \
                     .withColumn('windows', F.col('data.platforms.windows'))    
          


# COMMAND ----------

games_platform_df.show(10)

# COMMAND ----------

nblinux=games_platform_df.filter((games_platform_df.linux)).count()
nbmac=games_platform_df.filter((games_platform_df.mac)).count()
nbwindows=games_platform_df.filter((games_platform_df.windows)).count()
print(f" la répartion des jeux entre OS est linux games : {nblinux}, mac games : {nbmac}, windows games : {nbwindows}")


# COMMAND ----------

# MAGIC %md
# MAGIC Cela place bien évidement la plateforme Windows comme l'OS le plus représenté.

# COMMAND ----------

games_nb = df.select(F.col('id')).distinct().count()
games_linux_nb=games_platform_df.filter(F.col('linux')== True ).count()
games_mac_nb=games_platform_df.filter(F.col('mac')== True ).count()
games_windows_nb=games_platform_df.filter(F.col('windows')== True ).count()
games_adult_nb =df.filter( \
    (F.col('data.required_age')!='0') \
     & (F.col('data.required_age')!='3') \
     & (F.col('data.required_age')!='5') \
     & (F.col('data.required_age')!='6') \
     & (F.col('data.required_age')!='7+') \
     & (F.col('data.required_age')!='8') \
     & (F.col('data.required_age')!='9') \
     & (F.col('data.required_age')!='10') \
     & (F.col('data.required_age')!='12') \
     & (F.col('data.required_age')!='13') \
     & (F.col('data.required_age')!='14')
     ).count()

categorie_age_nb=df.select('data.required_age').distinct().count()
print("nb de jeu  : ",games_nb)
print("nb jeu windows : ",games_windows_nb,"   mac : ", games_mac_nb, "  linux : ",games_linux_nb)
print("nb de jeu pour adulte  : ",games_adult_nb)
print("nombre de catégorie d'age : ",categorie_age_nb)
df.select('data.required_age').distinct().show()
#truc_df.show(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regrouppement des jeux par tranche d'âge.

# COMMAND ----------

result_required_age=df.groupBy(F.col('data.required_age')).count().orderBy(F.desc("data.required_age"))
display(result_required_age)


# COMMAND ----------

# MAGIC %md
# MAGIC Il est à noter que l'âge de 180 ans est une abberration.  Cependant on distingue 21 catégories d'âge.

# COMMAND ----------

display(categorie_age_nb)

# COMMAND ----------

# MAGIC %md
# MAGIC Le nombre d'acteur producteur de jeu est assez élevé avec 29966 producteurs différents.
# MAGIC

# COMMAND ----------

df.select('data.publisher').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Les jeux se répartissent suivant des genres spéciaux.

# COMMAND ----------

result_publisher=games_df.groupBy('publisher').count().orderBy(F.desc("count")).limit(100)
display(result_publisher)


# COMMAND ----------

result_languages=games_df.groupBy('languages').count().orderBy(F.desc("count"))
display(result_languages)

# COMMAND ----------

display(games_df.groupBy('release_date').count())

# COMMAND ----------

discount_game=games_df.filter( games_df.discount>0).count()
print(" il y a ",discount_game, " sur ", games_nb, " jeux soit ",100.0*discount_game/games_nb, "%")

# COMMAND ----------

# MAGIC %md
# MAGIC Réponse à la liste des questions du chef :
# MAGIC
# MAGIC ### Analyse Macro du marché 
# MAGIC
# MAGIC Q : _Quel éditeur a sorti le plus de jeux sur Steam ?_ 
# MAGIC
# MAGIC A : Il s'agit de l'éditeur Big Fish Games qui a sorti le plus de jeux sur Steam.
# MAGIC
# MAGIC Q : _Quels sont les jeux les mieux classés ?_
# MAGIC
# MAGIC A: the top rated games on steam platform are :
# MAGIC - Counter-Strike: Global Offensive
# MAGIC - Dota 2
# MAGIC - Grand Theft Auto 
# MAGIC - PUBG: BATTLEGROUNDS
# MAGIC - Terraria
# MAGIC - Tom Clancy's Rain...
# MAGIC - Garry's Mod
# MAGIC - Team Fortress 2
# MAGIC - Rust
# MAGIC - Left 4 Dead 2
# MAGIC - The Witcher 3: Wi..
# MAGIC - Among Us
# MAGIC - Euro Truck Simula...
# MAGIC - Wallpaper Engine
# MAGIC - PAYDAY 2
# MAGIC la tendance du gameplay est clairement sur des jeus de type shooters, avec une forte présence retours des jeux AAA militaires
# MAGIC
# MAGIC Q : _Quelles sont les années avec le plus de sorties?_ 
# MAGIC
# MAGIC A : Les années de pic sont 2021 et 2020. Juste après viennent 2019 et 2022, qui affichent également un bon nombre de sorties.
# MAGIC
# MAGIC
# MAGIC Q: _Y a t'il eu plus ou moins de sortie de jeux pendant la pandémie de la COVID ?_
# MAGIC
# MAGIC A : Dans la période la pire de la pandémie COVIDT (2020_2021), c'est en effet le moment où il y a eu le plus de sortie. Cette périoide de repli sur soi a été propice à l'explosion de contact "virtuel" via le jeu vidéo.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Q : _Comment sont distribué les prix ?_ 
# MAGIC
# MAGIC A : les prix sont distribués en dessous de 1000. Le gros semble nul.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import DoubleType
games_df = games_df.withColumn("price", games_df["price"].cast(DoubleType()))
display(games_df.groupBy('price').count().sort('count', ascending=True))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Q : _Y a t'il beaucoup de jeux en discount ?_
# MAGIC
# MAGIC A : il y a  2518  sur  55691  jeux soit  4.521376883158859%. Cela ne représente pas beaucoup de jeu. Le monde du jeu video n'exploita pas trop le filon des soldes pour augmenter ses ventes.
# MAGIC
# MAGIC
# MAGIC Q : _Quel est la langue la plus représentée dans les jeux ?_
# MAGIC
# MAGIC A : Tout comme sur beaucoup de sujet, c'est l'anglais qui domine le monde du jeu video.
# MAGIC
# MAGIC
# MAGIC Q : _Y a t'il beaucoup de jeux interdit pour les enfants de moins de 16/18 ans ?_
# MAGIC
# MAGIC A : Non sur 55691 seulement 573 sont reférencés pour 16 ans et plus.
# MAGIC
# MAGIC

# COMMAND ----------

games_df.select('release_date').show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyse des Genres 
# MAGIC
# MAGIC Quels sont les genres les plus représentés ?
# MAGIC Ci-dessous on obtient la liste des genres les plus représentés sur Steam par ordre décroissant.
# MAGIC
# MAGIC

# COMMAND ----------

result_genre=games_df.groupBy(F.col('genre')).count().orderBy(F.desc("count")).limit(50)
display(result_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC Le genre le plus représenté chez Steam est l'Action Indie.
# MAGIC
# MAGIC Nous poursuivons en trouvant la répartition entre les genres de jeu. On obtient :

# COMMAND ----------

from pyspark.sql.functions import split
# a faire absoleument
#s=set()
#(s.add(ele) for ele in games_df.select(split(games_df.genre, ',').alias('nomtype')).show())
#games_df.select(split(games_df.genre, ',')).show()
uniques=games_df.select(split(games_df.genre, ',').alias('nomtype')).select('*', F.explode('nomtype').alias('genre_exploded'))
genre_unique_set = {str(row.genre_exploded).strip() for row in uniques.collect()}
action=games_df.filter(games_df.genre.contains("Action")).count()
simulation=games_df.filter(games_df.genre.contains("Simulation")).count()
adventure=games_df.filter(games_df.genre.contains("Adventure")).count()
strategy=games_df.filter(games_df.genre.contains("Strategy")).count()
indie=games_df.filter(games_df.genre.contains("Indie")).count()
casual=games_df.filter(games_df.genre.contains("Casual")).count()
print(f"action {action}, simulation {simulation}, adventure {adventure}, strategy {strategy}, indie {indie}, casual {casual} ")


# COMMAND ----------

genre_unique_set

# COMMAND ----------

genre_dict={}
for genre in genre_unique_set:
    genre_dict[genre]=games_df.filter(games_df.genre.contains(genre)).count()                               
display(genre_dict)

# COMMAND ----------

for genre in genre_unique_set:
    print(f" Le genre {genre} a {genre_dict[genre]} jeux.")

# COMMAND ----------

# MAGIC %md
# MAGIC Y a t'il des genres qui ont un meilleur ration de revu positif/négatif ? 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

genre_ratio={}
for genre in genre_unique_set:
    positive=games_df.filter(games_df.genre.contains(genre)).agg(F.sum("positive")).collect()[0][0]
    negative=games_df.filter(games_df.genre.contains(genre)).agg(F.sum("negative")).collect()[0][0]
    genre_ratio[genre]={"positive": positive,"negative": negative, "ratio":(float(positive)/float(negative)) }
genre_ratio


# COMMAND ----------

# MAGIC %md
# MAGIC - Certains éditeurs ont-ils des genres favoris ?
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC - Quels sont les genres les plus lucratifs ?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Analyse suivant les Platformes de système d'exploitation 
# MAGIC
# MAGIC Sur quelle plateforme Windows/Mac/Linux y a t'il de jeux présents sur Steam ?

# COMMAND ----------

nblinux=games_platform_df.filter((games_platform_df.linux)).count()
nbmac=games_platform_df.filter((games_platform_df.mac)).count()
nbwindows=games_platform_df.filter((games_platform_df.windows)).count()
print(f" linux games : {nblinux}, mac games : {nbmac}, windows games : {nbwindows}")

# COMMAND ----------

# MAGIC %md
# MAGIC Sans réelle suprise la plateforme la plus prisée pour les jeux video est Windows.

# COMMAND ----------

# MAGIC %md
# MAGIC Certains genres ont-ils tendance à être préférentiellement disponibles sur certaines plateformes ?
# MAGIC

# COMMAND ----------



for genre in genre_unique_set:
    windows=df.filter (df.data.genre.contains(genre) & (df.data.platforms.windows)).count()
    linux=df.filter( df.data.genre.contains(genre)  & (df.data.platforms.linux)).count()
    macosx=df.filter(df.data.genre.contains(genre)  & (df.data.platforms.mac)).count()
    print("répartition des OS pour le genre :",genre, "Windows : ", windows," Linux : ", linux, " Mac OS X : ", macosx )


# COMMAND ----------

