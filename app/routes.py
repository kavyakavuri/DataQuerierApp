from app import app
from flask import render_template, request, json
from mysql.connector import connect,Error
import redshift_connector
import pymssql
import pyodbc
import time

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/querypage')
def querypage():
    return render_template('querypage.html',output = {}, time_elapsed = '-')

def mysqlquery(subject):
    try:
        with connect(
            host = 'database-1.caytflhlgy1t.us-east-2.rds.amazonaws.com',
            port = '3306',
            user = 'admin',
            database ='instacartNormalised',
            password = 'rutgers21'
        ) as connection:
            with connection.cursor() as cursor:
                start = time.time()
                cursor.execute(subject)
                print("subject:", subject)
                result = cursor.fetchall()
                result.insert(0,cursor.column_names)
                end = time.time()
                time_elapsed = end - start
                return "",result,time_elapsed
    except Error as e:
        print(e)
        return str(e.msg),"",0

def mssqlquery(subject):
    try:
        conn = pymssql.connect('database-2.caytflhlgy1t.us-east-2.rds.amazonaws.com', 'admin', 'rutgers21', 'adnimerge')
        cursor: pymssql.Cursor = conn.cursor()
        print("subject:", subject)
        start = time.time()
        cursor.execute(subject)
        result = cursor.fetchall()
        desc = cursor.description
        cols = []
        for col in desc:
            cols.append(col[0])
        result.insert(0,cols)
        end = time.time()
        time_elapsed = end - start
        end = time.time()
        time_elapsed = end - start
        return "",result, time_elapsed
    except Exception as e:
        print(e)
        return str(e),"",0

def redshiftquery(subject):
    try:
        conn = redshift_connector.connect(
                host = 'project1-redshift.chxau5c0xldf.us-east-2.redshift.amazonaws.com',
                port = 5439,
                user = 'awsuser',
                database ='dev',
                password = 'Rutgers21'
            )
        cursor: redshift_connector.Cursor = conn.cursor()
        print("subject:", subject)
        start = time.time()
        cursor.execute(subject)
        result: pd.DataFrame = cursor.fetch_dataframe()
        data =  result.values.tolist()
        col = (result.columns.values)
        col = list(col)
        data.insert(0,col)
        data2 = tuple(data)
        end = time.time()
        time_elapsed = end - start
        return "",data2, time_elapsed
    except Exception as e:
        print(e)
        #ex = json.loads(str(e).replace("'", '"').replace("\n", "\\n"))['M']
        #idx1 = str(e).index('M')
        #idx2 =  str(e).index('F')
        #ex = str(e)[idx1+3:idx2-2]
        return str(e),"",0

def mongoquery(subject):
    try:
        con = pyodbc.connect('DRIVER={Devart ODBC Driver for MongoDB};'
                                            'Server=127.0.0.1;'
                                            'Port=27017;'
                                            'Database=adnimerge')
        start = time.time()
        cursor = con.cursor()
        print("subject:", subject)
        cursor.execute(subject)
        result = cursor.fetchall()
        end = time.time()
        time_elapsed = end - start
        data = []
        columns = [column[0] for column in cursor.description]
        data.append(columns)
        for row in result:
            data.append(list(row))
        return "",data, time_elapsed
    except pyodbc.Error as e:
        sqlstate = e.args[1]
        return sqlstate,"",0

@app.route('/submitquery', methods=['POST'])
def submitquery():
    if request.method == 'POST':
        req = request.get_json()
        dbms = req['dbms']
        subject = req['subject']
        if dbms == 'MySQL':
            error_ret, result, time_elapsed = mysqlquery(subject)
        elif dbms == 'RedShift':
            error_ret, result, time_elapsed = redshiftquery(subject)
        elif dbms == 'MSSQL':
            error_ret, result, time_elapsed = mssqlquery(subject)
        else:
            error_ret, result, time_elapsed = mongoquery(subject)
    return json.jsonify(error_returned = error_ret, output = result, time_elapsed = str(time_elapsed))