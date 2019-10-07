data = [["key", "integer", "False"], ["value", "string", "False"]]


print("fields = [")
for i in data:
	types = ""
	if i[1]=="integer" :
		types = "IntegerType()"
	elif i[1]=="string" :
		types = "StringType()"

	print("StructField('"+i[0]+"', "+types+", "+i[2]+"),")
print("]")
"""
fields = [
        StructField('key', IntegerType(), False),
        StructField('value', StringType(), False)
    ]
"""