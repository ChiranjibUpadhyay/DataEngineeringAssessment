import mysql.connector

try:
    #creates a connection object for database
    conn=mysql.connector.connect(user='root',password='*********',host='localhost',database='incubyte')
except:
    print("Connection not successful")

print("Connection Successful")

#creating a cursor object for accessing and querying databse
cursor=conn.cursor()

sql1='''CREATE TABLE Table_Global(
    Ch CHAR,
    H CHAR,
    Customer_Name VARCHAR(255) NOT NULL,
    Customer_Id VARCHAR(255) NOT NULL,
    Open_Date DATE NOT NULL,
    Last_Consulted_Date DATE,
    Vaccination_Id CHAR(5),
    Dr_Name CHAR(255),
    State CHAR(5),
    Country CHAR(5),
    DOB DATE,
    Is_Active CHAR,
    PRIMARY KEY(Customer_Name)
    )'''
cursor.execute(sql1)

sql2='''LOAD DATA INFILE '/var/lib/mysql-files/Customers.csv'
        INTO TABLE Table_Global
        FIELDS TERMINATED BY '|'
        IGNORE 1 ROWS
        (Ch,H,Customer_Name,Customer_Id,Open_Date,Last_Consulted_Date,Vaccination_Id,Dr_Name,State,Country,@DOB,Is_Active)
        SET DOB=STR_TO_DATE(@DOB,'%m%d%Y')'''
cursor.execute(sql2)

sql3='''ALTER TABLE Table_Global DROP COLUMN Ch,DROP COLUMN H'''
cursor.execute(sql3)

# cursor.execute('''SELECT * FROM Table_Global''')
# result=cursor.fetchall()
# print(result)
sql4='''SELECT DISTINCT Country FROM Table_Global'''
cursor.execute(sql4)
Countries=cursor.fetchall()
# print(Countries)
for x in Countries:
    T='Table_'+x[0]
    sql5="CREATE TABLE "+T+" LIKE Table_Global"
    cursor.execute(sql5)
    sql6="INSERT INTO "+T+" SELECT * FROM Table_Global WHERE Country='%s'"%(x[0])
    cursor.execute(sql6)

conn.close()
