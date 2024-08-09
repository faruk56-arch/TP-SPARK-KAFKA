from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp, count, avg, max, desc, when, date_format
from pyspark.sql.functions import corr


# Exercice 2 : Importation et Exploration des Donnees Sismiques
# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("SismicDataAnalysis") \
    .getOrCreate()

# Chemins des fichiers CSV dans HDFS
sismic_data_path = "hdfs://namenode:9000/dataset_sismique.csv"
sismic_villes_path = "hdfs://namenode:9000/dataset_sismique_villes.csv"

# Charger les donnees sismiques depuis HDFS
sismic_data = spark.read.csv(sismic_data_path, header=True, inferSchema=True)
sismic_villes = spark.read.csv(sismic_villes_path, header=True, inferSchema=True)

# Afficher la structure des donnees
# sismic_data.printSchema()
# sismic_data.show(5)
# sismic_villes.printSchema()
# sismic_villes.show(5)
print(sismic_data.columns)



# Exercice 3 : Nettoyage des Donnees avec Spark
cleaned_data = sismic_data \
    .filter("magnitude is not null and `tension entre plaque` is not null and secousse is not null and date is not null") \
    .withColumn("timestamp", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("secousse", when(col("secousse") == "true", 1).otherwise(0)) 

# cleaned_data.write.csv("hdfs://namenode:9000/cleaned_sismic_data.csv", header=True, mode="overwrite")
# Lire les donnees pour verifier
# read_cleaned_data = spark.read.csv("hdfs://namenode:9000/cleaned_sismic_data.csv", header=True, inferSchema=True)

# cleaned_data.write.mode("overwrite").saveAsTable("cleaned_sismic_data")
# cleaned_data.show(5)
# Sauvegarder les donnees nettoyees


# Exercice 4 : Analyse Preliminaire des evenements Sismiques
events_by_date = cleaned_data.groupBy("timestamp").agg(
    count("*").alias("event_count"),
    avg("magnitude").alias("avg_magnitude"),
    max("magnitude").alias("max_magnitude")
)
# Afficher les resultats
events_by_date.orderBy(desc("event_count")).show(10)

# Periodes d'activite sismique importante par nombre d'evenements
important_activity_by_event_count = events_by_date.orderBy(desc("event_count"))
important_activity_by_event_count.show(10)

# Periodes d'activite sismique importante par magnitude maximale
important_activity_by_max_magnitude = events_by_date.orderBy(desc("max_magnitude"))
important_activity_by_max_magnitude.show(10)


# Exercice 5 : Correlation des Evenements Sismiques

correlation_secousse_magnitude = cleaned_data.stat.corr("secousse", "magnitude")
correlation_tension_magnitude = cleaned_data.stat.corr("tension entre plaque", "magnitude")
correlation_secousse_tension = cleaned_data.stat.corr("secousse", "tension entre plaque")

print("Correlation entre secousse et magnitude: {}".format(correlation_secousse_magnitude))
print("Correlation entre tension entre plaque et magnitude: {}".format(correlation_tension_magnitude))
print("Correlation entre secousse et tension entre plaque: {}".format(correlation_secousse_tension))

#Exercice 6 : Agregation des Donnees Sismiques

monthly_aggregation = cleaned_data \
    .withColumn("month", date_format(col("timestamp"), "yyyy-MM")) \
    .groupBy("month") \
    .agg(
        count("*").alias("event_count"),
        avg("magnitude").alias("avg_magnitude"),
        max("magnitude").alias("max_magnitude")
    )

monthly_aggregation.orderBy("month").show(10)































# # Analyse des evenements sismiques
# events_by_date = cleaned_data.groupBy("date").agg(
#     count("*").alias("event_count"),
#     avg("magnitude").alias("avg_magnitude"),
#     max("magnitude").alias("max_magnitude")
# )

# # Afficher les resultats
# events_by_date.orderBy(col("event_count").desc()).show(10)
