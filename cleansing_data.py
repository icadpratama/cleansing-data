from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from src.utils.spark_factory import get_spark

spark = get_spark()

# Fungsi untuk get data dari tabel asal
def select_data():
  item_requests = spark.sql("SELECT * FROM ticket").collect() # Sintaks untuk mengubah DataFrame ke list
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
    sc = pyspark.context.SparkContext(master='host', appName='Sample App')
    session = pyspark.sql.session.SparkSession(sc)
    
    session = SparkSession.builder.enableHiveSupport().getOrCreate()
    session.sql('TRUNCATE TABLE ticket_temp')

# Insert data ke table baru
def insert_data():
    spark.sql("CREATE TABLE IF NOT EXISTS ticket_temp (account_id STRING, account_name STRING) USING hive") # urutan field disesuaikan dengan ururtan list

    fields = [
        StructField('account_id', StringType(), True),
        StructField('account_name', StringType(), True)
    ] # Urutan StructField disesuaikan dengan urutan field pada CREATE TABLE

    list = select_data() # variable list diisi dengan nilai dari fungsi select_data()
    data = check_duplicate(list) # variable data diisi dengan data yang telah dipisah
    schema = StructType(fields) # Membuat schema baru pada Apache Hive
    df = spark.createDataFrame(data, schema) # membuat DataFrame berdasarkan data bersih dan schema baru
    df.write.insertInto("ticket_temp", overwrite = False) # Insert ke table baru

# Memanggil atau menjalankan fungsi insert_data
insert_data()

# List of Type
# fields = [
#     StructField('id', IntegerType(), False),  
#     StructField('uuid', StringType(), False),  
#     StructField('description', TextType(), False), 
#     StructField('price', DecimalType(precision=10, scale=2), False),
#     StructField('some_date', DateType(), False),  
#     StructField('some_timestamp', TimestampType(), False),
#     StructField('binary_file', BinaryType(), False),  
#     StructField('deleted', BooleanType(), False),
#     StructField('tags', ArrayType(StringType()), False),
#     StructField('metadata', StructType(), False)
# ]

# data = [
#     {
#         'id': 123,
#         'uuid': '123e4567-e89b-12d3-a456',
#         'description': 'lorem ipsum',
#         'price': 45.67,
#         'some_date': date_object,
#         'some_timestamp': datetime_object,
#         'binary_file': b'binary-encoding',
#         'deleted': False,
#         'tags': ['tag1', 'tag2', 'tag3'],
#         'metadata': {
#             'source': 'universe',
#             'original_price': 33,
#         }
#     },
#     # ...
# ]