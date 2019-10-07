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
	StructField('business_date', StringType(), True),
]