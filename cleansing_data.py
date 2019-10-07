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
        account_screen,after_call_duration,answer,appraised,assign_duration,assigned_agent,assigned_date,assigned_date_str,audio_link,bot_id,
        call_duration,call_finish_date,call_transfer,caller_state,channel,channel_key,channel_type,close_interval,closed_by,closed_date,
        closed_date_str,closure_type,contact_id, created_date,created_date_str,customer_rating,document_link,embed_html,error_code,error_message,
        error_shown, escalated,escalated_date,escalated_from,escalated_to,follower_count,friend_count,from_group,greeting_status,id,incoming,latitude,
        longitude,mention,message,message_id,notified,offline_date,online,online_date,online_duration,open_date,open_date_str,open_duration,
        outgoing_call,parent,pending_date,pending_date_str,pending_duration,picture_link,post,priority,profile_link,reassigned,reassigned_str,
        redistribute,redistribute_agent,remark,reply_agent,reply_cc,response_time,response_time_agent,rule_id,rule_owner,spell,status,subject,
        subject_changed,supervisor,ticket_number,ticket_owner,to_group,transfered,transfered_date,transfered_date_str,transfered_from,transfered_str,
        transfered_to,unassign_duration,unassigned_date,unassigned_date_str,video_link,within_sla,business_date 
    FROM ticket
  """).collect() # Sintaks untuk mengubah DataFrame ke list
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
                    account_screen STRING,
                    after_call_duration STRING,
                    answer STRING,
                    appraised STRING,
                    assign_duration STRING,
                    assigned_agent STRING,
                    assigned_date STRING,
                    assigned_date_str STRING,
                    audio_link STRING,
                    bot_id STRING,
                    call_duration STRING,
                    call_finish_date STRING,
                    call_transfer STRING,
                    caller_state STRING,
                    channel STRING,
                    channel_key STRING,
                    channel_type STRING,
                    close_interval STRING,
                    closed_by STRING,
                    closed_date STRING,
                    closed_date_str STRING,
                    closure_type STRING,
                    contact_id STRING,
                    created_date STRING,
                    created_date_str STRING,
                    customer_rating STRING,
                    document_link STRING,
                    embed_html STRING,
                    error_code STRING,
                    error_message STRING,
                    error_shown STRING,
                    escalated STRING,
                    escalated_date STRING,
                    escalated_from STRING,
                    escalated_to STRING,
                    follower_count STRING,
                    friend_count STRING,
                    from_group STRING,
                    greeting_status STRING,
                    id STRING,
                    incoming STRING,
                    latitude STRING,
                    longitude STRING,
                    mention STRING,
                    message STRING,
                    message_id STRING,
                    notified STRING,
                    offline_date STRING,
                    online STRING,
                    online_date STRING,
                    online_duration STRING,
                    open_date STRING,
                    open_date_str STRING,
                    open_duration STRING,
                    outgoing_call STRING,
                    parent STRING,
                    pending_date STRING,
                    pending_date_str STRING,
                    pending_duration STRING,
                    picture_link STRING,
                    post STRING,
                    priority STRING,
                    profile_link STRING,
                    reassigned STRING,
                    reassigned_str STRING,
                    redistribute STRING,
                    redistribute_agent STRING,
                    remark STRING,
                    reply_agent STRING,
                    reply_cc STRING,
                    response_time STRING,
                    response_time_agent STRING,
                    rule_id STRING,
                    rule_owner STRING,
                    spell STRING,
                    status STRING,
                    subject STRING,
                    subject_changed STRING,
                    supervisor STRING,
                    ticket_number STRING,
                    ticket_owner STRING,
                    to_group STRING,
                    transfered STRING,
                    transfered_date STRING,
                    transfered_date_str STRING,
                    transfered_from STRING,
                    transfered_str STRING,
                    transfered_to STRING,
                    unassign_duration STRING,
                    unassigned_date STRING,
                    unassigned_date_str STRING,
                    video_link STRING,
                    within_sla STRING,
                    business_date STRING,) USING hive""")

    fields = [
        StructField('account_screen', StringType(), True),
        StructField('after_call_duration', StringType(), True),
        StructField('answer', StringType(), True),
        StructField('appraised', StringType(), True),
        StructField('assign_duration', StringType(), True),
        StructField('assigned_agent', StringType(), True),
        StructField('assigned_date', StringType(), True),
        StructField('assigned_date_str', StringType(), True),
        StructField('audio_link', StringType(), True),
        StructField('bot_id', StringType(), True),
        StructField('call_duration', StringType(), True),
        StructField('call_finish_date', StringType(), True),
        StructField('call_transfer', StringType(), True),
        StructField('caller_state', StringType(), True),
        StructField('channel', StringType(), True),
        StructField('channel_key', StringType(), True),
        StructField('channel_type', StringType(), True),
        StructField('close_interval', StringType(), True),
        StructField('closed_by', StringType(), True),
        StructField('closed_date', StringType(), True),
        StructField('closed_date_str', StringType(), True),
        StructField('closure_type', StringType(), True),
        StructField('contact_id', StringType(), True),
        StructField('created_date', StringType(), True),
        StructField('created_date_str', StringType(), True),
        StructField('customer_rating', StringType(), True),
        StructField('document_link', StringType(), True),
        StructField('embed_html', StringType(), True),
        StructField('error_code', StringType(), True),
        StructField('error_message', StringType(), True),
        StructField('error_shown', StringType(), True),
        StructField('escalated', StringType(), True),
        StructField('escalated_date', StringType(), True),
        StructField('escalated_from', StringType(), True),
        StructField('escalated_to', StringType(), True),
        StructField('follower_count', StringType(), True),
        StructField('friend_count', StringType(), True),
        StructField('from_group', StringType(), True),
        StructField('greeting_status', StringType(), True),
        StructField('id', StringType(), True),
        StructField('incoming', StringType(), True),
        StructField('latitude', StringType(), True),
        StructField('longitude', StringType(), True),
        StructField('mention', StringType(), True),
        StructField('message', StringType(), True),
        StructField('message_id', StringType(), True),
        StructField('notified', StringType(), True),
        StructField('offline_date', StringType(), True),
        StructField('online', StringType(), True),
        StructField('online_date', StringType(), True),
        StructField('online_duration', StringType(), True),
        StructField('open_date', StringType(), True),
        StructField('open_date_str', StringType(), True),
        StructField('open_duration', StringType(), True),
        StructField('outgoing_call', StringType(), True),
        StructField('parent', StringType(), True),
        StructField('pending_date', StringType(), True),
        StructField('pending_date_str', StringType(), True),
        StructField('pending_duration', StringType(), True),
        StructField('picture_link', StringType(), True),
        StructField('post', StringType(), True),
        StructField('priority', StringType(), True),
        StructField('profile_link', StringType(), True),
        StructField('reassigned', StringType(), True),
        StructField('reassigned_str', StringType(), True),
        StructField('redistribute', StringType(), True),
        StructField('redistribute_agent', StringType(), True),
        StructField('remark', StringType(), True),
        StructField('reply_agent', StringType(), True),
        StructField('reply_cc', StringType(), True),
        StructField('response_time', StringType(), True),
        StructField('response_time_agent', StringType(), True),
        StructField('rule_id', StringType(), True),
        StructField('rule_owner', StringType(), True),
        StructField('spell', StringType(), True),
        StructField('status', StringType(), True),
        StructField('subject', StringType(), True),
        StructField('subject_changed', StringType(), True),
        StructField('supervisor', StringType(), True),
        StructField('ticket_number', StringType(), True),
        StructField('ticket_owner', StringType(), True),
        StructField('to_group', StringType(), True),
        StructField('transfered', StringType(), True),
        StructField('transfered_date', StringType(), True),
        StructField('transfered_date_str', StringType(), True),
        StructField('transfered_from', StringType(), True),
        StructField('transfered_str', StringType(), True),
        StructField('transfered_to', StringType(), True),
        StructField('unassign_duration', StringType(), True),
        StructField('unassigned_date', StringType(), True),
        StructField('unassigned_date_str', StringType(), True),
        StructField('video_link', StringType(), True),
        StructField('within_sla', StringType(), True),
        StructField('business_date', StringType(), True)
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