from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from src.utils.spark_factory import get_spark

spark = get_spark()

# Fungsi untuk get data dari tabel asal
def select_data():
  item_requests = spark.sql("""
    SELECT 
        account_id,account_name,account_screen,after_call_duration,answer,appraised,assign_duration,assigned_agent,
        assigned_date,assigned_date_str,audio_link,bot_id,call_duration,call_finish_date,call_transfer,caller_state,
        channel,channel_key,channel_type,close_interval,closed_by,closed_date,closed_date_str,closure_type,contact_id,
        created_date,created_date_str,customer_rating,document_link,embed_html,error_code,error_message,error_shown,
        escalated,escalated_date,escalated_from,escalated_to,follower_count,friend_count,from_group,greeting_status,id,
        incoming,latitude,longitude,mention,message,message_id,notified,offline_date,online,online_date,online_duration,
        open_date,open_date_str,open_duration,outgoing_call,parent,pending_date,pending_date_str,pending_duration,
        picture_link,post,priority,profile_link,reassigned,reassigned_str,redistribute,redistribute_agent,
        remark,reply_agent,reply_cc,response_time,response_time_agent,rule_id,rule_owner,spell,status,subject,
        subject_changed,supervisor,ticket_number,ticket_owner,to_group,transfered,transfered_date,transfered_date_str,
        transfered_from,transfered_str,transfered_to,unassign_duration,unassigned_date,unassigned_date_str,video_link,
        within_sla FROM ticket""").collect() # Sintaks untuk mengubah DataFrame ke list
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
    # urutan field disesuaikan dengan urutan list pada saat select data
    spark.sql("""CREATE TABLE IF NOT EXISTS ticket_temp (
                    `account_id` string, 
                    `account_name` string, 
                    `account_screen` string, 
                    `after_call_duration` bigint, 
                    `answer` boolean, 
                    `appraised` boolean, 
                    `assign_duration` bigint, 
                    `assigned_agent` string, 
                    `assigned_date` timestamp, 
                    `assigned_date_str` string, 
                    `audio_link` string, 
                    `bot_id` string, 
                    `call_duration` bigint, 
                    `call_finish_date` timestamp, 
                    `call_transfer` boolean, 
                    `caller_state` string, 
                    `channel` string, 
                    `channel_key` string, 
                    `channel_type` string, 
                    `close_interval` bigint, 
                    `closed_by` string, 
                    `closed_date` timestamp, 
                    `closed_date_str` string, 
                    `closure_type` string, 
                    `contact_id` string, 
                    `created_date` timestamp, 
                    `created_date_str` string, 
                    `customer_rating` bigint, 
                    `document_link` string, 
                    `embed_html` string, 
                    `error_code` string, 
                    `error_message` string, 
                    `error_shown` boolean, 
                    `escalated` boolean, 
                    `escalated_date` timestamp, 
                    `escalated_from` string, 
                    `escalated_to` string, 
                    `follower_count` bigint, 
                    `friend_count` bigint, 
                    `from_group` string, 
                    `greeting_status` boolean, 
                    `id` string, 
                    `incoming` boolean, 
                    `latitude` double, 
                    `longitude` double, 
                    `mention` boolean, 
                    `message` string, 
                    `message_id` string, 
                    `notified` boolean, 
                    `offline_date` timestamp, 
                    `online` boolean, 
                    `online_date` timestamp, 
                    `online_duration` bigint, 
                    `open_date` timestamp, 
                    `open_date_str` string, 
                    `open_duration` bigint, 
                    `outgoing_call` boolean, 
                    `parent` bigint, 
                    `pending_date` timestamp, 
                    `pending_date_str` string, 
                    `pending_duration` bigint, 
                    `picture_link` string, 
                    `post` boolean, 
                    `priority` boolean, 
                    `profile_link` string, 
                    `reassigned` boolean, 
                    `reassigned_str` string, 
                    `redistribute` boolean, 
                    `redistribute_agent` string, 
                    `remark` string, 
                    `reply_agent` string, 
                    `reply_cc` string, 
                    `response_time` bigint, 
                    `response_time_agent` bigint, 
                    `rule_id` string, 
                    `rule_owner` string, 
                    `spell` string, 
                    `status` string, 
                    `subject` string, 
                    `subject_changed` boolean, 
                    `supervisor` string, 
                    `ticket_number` string, 
                    `ticket_owner` string, 
                    `to_group` string, 
                    `transfered` boolean, 
                    `transfered_date` timestamp, 
                    `transfered_date_str` string, 
                    `transfered_from` string, 
                    `transfered_str` string, 
                    `transfered_to` string, 
                    `unassign_duration` bigint, 
                    `unassigned_date` timestamp, 
                    `unassigned_date_str` string, 
                    `video_link` string, 
                    `within_sla` boolean)) USING hive""")

    fields = [
        StructField('account_id', StringType(), True),
        StructField('account_name', StringType(), True),
        StructField('account_screen', StringType(), True),
        StructField('after_call_duration', LongType(), True),
        StructField('answer', BooleanType(), True),
        StructField('appraised', BooleanType(), True),
        StructField('assign_duration', LongType(), True),
        StructField('assigned_agent', StringType(), True),
        StructField('assigned_date', TimestampType(), True),
        StructField('assigned_date_str', StringType(), True),
        StructField('audio_link', StringType(), True),
        StructField('bot_id', StringType(), True),
        StructField('call_duration', LongType(), True),
        StructField('call_finish_date', TimestampType(), True),
        StructField('call_transfer', BooleanType(), True),
        StructField('caller_state', StringType(), True),
        StructField('channel', StringType(), True),
        StructField('channel_key', StringType(), True),
        StructField('channel_type', StringType(), True),
        StructField('close_interval', LongType(), True),
        StructField('closed_by', StringType(), True),
        StructField('closed_date', TimestampType(), True),
        StructField('closed_date_str', StringType(), True),
        StructField('closure_type', StringType(), True),
        StructField('contact_id', StringType(), True),
        StructField('created_date', TimestampType(), True),
        StructField('created_date_str', StringType(), True),
        StructField('customer_rating', LongType(), True),
        StructField('document_link', LongType(), True),
        StructField('embed_html', LongType(), True),
        StructField('error_code', LongType(), True),
        StructField('error_message', LongType(), True),
        StructField('error_shown', BooleanType(), True),
        StructField('escalated', BooleanType(), True),
        StructField('escalated_date', TimestampType(), True),
        StructField('escalated_from', StringType(), True),
        StructField('escalated_to', StringType(), True),
        StructField('follower_count', LongType(), True),
        StructField('friend_count', LongType(), True),
        StructField('from_group', StringType(), True),
        StructField('greeting_status', BooleanType(), True),
        StructField('id', StringType(), True),
        StructField('incoming', BooleanType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('mention', BooleanType(), True),
        StructField('message', StringType(), True),
        StructField('message_id', StringType(), True),
        StructField('notified', BooleanType(), True),
        StructField('offline_date', TimestampType(), True),
        StructField('online', BooleanType(), True),
        StructField('online_date', TimestampType(), True),
        StructField('online_duration', LongType(), True),
        StructField('open_date', TimestampType(), True),
        StructField('open_date_str', StringType(), True),
        StructField('open_duration', LongType(), True),
        StructField('outgoing_call', BooleanType(), True),
        StructField('parent', LongType(), True),
        StructField('pending_date', TimestampType(), True),
        StructField('pending_date_str', StringType(), True),
        StructField('pending_duration', LongType(), True),
        StructField('picture_link', StringType(), True),
        StructField('post', BooleanType(), True),
        StructField('priority', BooleanType(), True),
        StructField('profile_link', StringType(), True),

        StructField('reassigned', BooleanType(), True),
        StructField('reassigned_str', StringType(), True),
        StructField('redistribute', BooleanType(), True),
        StructField('redistribute_agent', StringType(), True),
        StructField('remark', StringType(), True),
        StructField('reply_agent', StringType(), True),
        StructField('reply_cc', StringType(), True),
        StructField('response_time', LongType(), True),
        StructField('response_time_agent', LongType(), True),
        StructField('rule_id', StringType(), True),
        StructField('spell', StringType(), True),
        StructField('status', StringType(), True),
        StructField('subject', StringType(), True),
        StructField('subject_changed', BooleanType(), True),
        StructField('supervisor', StringType(), True),
        StructField('ticket_number', StringType(), True),
        StructField('ticket_owner', StringType(), True),
        StructField('to_group', StringType(), True),
        StructField('transfered', BooleanType(), True),
        StructField('transfered_date', TimestampType(), True),
        StructField('transfered_date_str', StringType(), True),
        StructField('transfered_from', StringType(), True),
        StructField('transfered_str', StringType(), True),
        StructField('transfered_to', StringType(), True),
        StructField('unassign_duration', LongType(), True),
        StructField('unassigned_date', TimestampType(), True),
        StructField('unassigned_date_str', StringType(), True),
        StructField('video_link', StringType(), True),
        StructField('within_sla', BooleanType(), True)
    ]  # Urutan StructField disesuaikan dengan urutan field pada CREATE TABLE

    list = select_data() # variable list diisi dengan nilai dari fungsi select_data()
    data = check_duplicate(list) # variable data diisi dengan data yang telah dipisah
    schema = StructType(fields) # Membuat schema baru pada Apache Hive
    df = spark.createDataFrame(data, schema) # membuat DataFrame berdasarkan data bersih dan schema baru
    df.write.insertInto("ticket_temp", overwrite = False) # Insert ke table baru

# Memanggil atau menjalankan fungsi insert_data
insert_data()

# List of Type
# fields = [
#     StructField('id', LongType(), False),  
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
