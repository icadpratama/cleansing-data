import psycopg2

connection = psycopg2.connect(user = "postgres", password = "admin", host = "127.0.0.1", port = "5432", database = "linorce")
cursor = connection.cursor()

# Sumber data
def select_data():
    postgreSQL_select_Query = "SELECT name, details, quantity FROM item_requests"

    cursor.execute(postgreSQL_select_Query)
    item_requests = cursor.fetchall()
    listData = [];

    for row in item_requests:
        listData.append(row);

    return listData

# Pengecekan data yang duplikat
def check_duplicate(l):
    uniq = []
    for i in l:
        if not i in uniq:
            uniq.append(i)

    return uniq


# Menghapus semua data di table tujuan
def truncate_data():
    sql = "TRUNCATE TABLE item_requests_temp"
    cursor.execute(sql)
    connection.commit()

# Insert data ke table baru
def insert_data(data):
    sql = "INSERT INTO item_requests_temp(name, details, quantity) VALUES(%s, %s, %s)"
    cursor.executemany(sql,data) # batch insert postgresql
    connection.commit()


data = select_data() # data hasil query berupa list yang diassign ke variabel data
# truncate_data() # Menghapus data
insert_data(check_duplicate(data)) # insert data ke table tujuan
# truncate_data()

cursor.close();
connection.close();