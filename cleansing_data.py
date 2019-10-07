from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

# Fungsi untuk get data dari tabel asal
def select_data():
  item_requests = spark.sql("SELECT * FROM src").collect() # Sintaks untuk mengubah DataFrame ke list
  listData = []
  for row in item_requests:
    listData.append(row)
  
  return listData

# Fungsi untuk mengecek data yang duplikat
def check_duplicate(l):
    uniq = []
    for i in l:
        if not i in uniq:
            uniq.append(i)
    return uniq

# Truncate table, digunakan untuk pengembangan
def truncate_table():
    session = SparkSession.builder.enableHiveSupport().getOrCreate()
    session.sql('TRUNCATE TABLE src_temp')

# Insert data ke table baru
def insert_data():
    spark.sql("CREATE TABLE IF NOT EXISTS src_temp (key INT, value STRING) USING hive") # urutan field disesuaikan dengan ururtan list

    fields = [
        StructField('key', IntegerType(), False),
        StructField('value', StringType(), False)
    ] # Urutan StructField disesuaikan dengan urutan field pada CREATE TABLE

    list = select_data() # variable list diisi dengan nilai dari fungsi select_data()
    data = check_duplicate(list) # variable data diisi dengan data yang telah dipisah
    schema = StructType(fields) # Membuat schema baru pada Apache Hive
    df = spark.createDataFrame(data, schema) # membuat DataFrame berdasarkan data bersih dan schema baru
    df.write.insertInto("src_temp", overwrite = False) # Insert ke table baru

# Memanggil atau menjalankan fungsi insert_data
insert_data()