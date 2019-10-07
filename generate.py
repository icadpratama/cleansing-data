f = open("StructField.py","w+")

list = ["account_screen","after_call_duration","answer","appraised","assign_duration","assigned_agent","assigned_date","assigned_date_str",
		"audio_link","bot_id","call_duration","call_finish_date","call_transfer","caller_state","channel","channel_key","channel_type",
		"close_interval","closed_by","closed_date","closed_date_str","closure_type","contact_id","created_date","created_date_str",
		"customer_rating","document_link","embed_html","error_code","error_message","error_shown","escalated","escalated_date",
		"escalated_from","escalated_to","follower_count","friend_count","from_group","greeting_status","id","incoming","latitude","longitude",
		"mention","message","message_id","notified","offline_date","online","online_date","online_duration","open_date","open_date_str",
		"open_duration","outgoing_call","parent","pending_date","pending_date_str","pending_duration","picture_link","post","priority",
		"profile_link","reassigned","reassigned_str","redistribute","redistribute_agent","remark","reply_agent","reply_cc","response_time",
		"response_time_agent","rule_id","rule_owner","spell","status","subject","subject_changed","supervisor","ticket_number","ticket_owner",
		"to_group","transfered","transfered_date","transfered_date_str","transfered_from","transfered_str","transfered_to","unassign_duration",
		"unassigned_date","unassigned_date_str","video_link","within_sla","business_date"]

f.write("spark.sql(\"\"\"CREATE TABLE IF NOT EXISTS ticket_temp (")
for x in range(len(list)): 
    # print(list[x])
    f.write("\n\t"+list[x]+" STRING,")
f.write(") USING hive\"\"\")")

f.write("")

f.write("\n\nfields = [")
for x in range(len(list)): 
    # print(list[x])
    f.write("\n\t"+"StructField('"+list[x]+"', StringType(), True),")
f.write("\n]")

# print("fields = [")
# for i in list:
# 	types = ""
# 	if i[1]=="integer" :
# 		types = "IntegerType()"
# 	elif i[1]=="string" :
# 		types = "StringType()"

# 	print("StructField('"+i[0]+"'"," "+types+""," "+i[2]+")","")
# print("]")
"""
fields = ["StructField('key'"," IntegerType()"," False)",""StructField('value'"," StringType()"," False)
    ]
"""