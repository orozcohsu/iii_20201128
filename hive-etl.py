from pyhive import hive
from datetime import *
from hdfs import *
import wget


#connect to hive
conn=hive.connect(host='172.17.0.2',database='iii',auth='NONE')
cur=conn.cursor()

#get week of year
today=datetime.today()
week_number=str(today.strftime("%U"))

#drop first then create
client = Client('http://master:50070')
client.delete('/iii-bigdata/data/'+week_number, recursive=True)

#create hdfs folder
client.makedirs('/iii-bigdata/data/'+week_number, permission = 777 )

#put log file
wget.download('https://raw.githubusercontent.com/orozcohsu/iii_20201128/main/customer_log_20201125.csv', out='/tmp/customer_log_20201125.csv')
client.upload('/iii-bigdata/data/'+week_number, '/tmp/customer_log_20201125.csv')

wget.download('https://raw.githubusercontent.com/orozcohsu/iii_20201128/main/customer_log_20201126.csv', out='/tmp/customer_log_20201126.csv')
client.upload('/iii-bigdata/data/'+week_number, '/tmp/customer_log_20201126.csv')

wget.download('https://raw.githubusercontent.com/orozcohsu/iii_20201128/main/customer_log_20201127.csv', out='/tmp/customer_log_20201127.csv')
client.upload('/iii-bigdata/data/'+week_number, '/tmp/customer_log_20201127.csv')

wget.download('https://raw.githubusercontent.com/orozcohsu/iii_20201128/main/customer_log_20201128.csv', out='/tmp/customer_log_20201128.csv')
client.upload('/iii-bigdata/data/'+week_number, '/tmp/customer_log_20201128.csv')

try:
    #alter partition (table: sales_log)
    cur.execute("alter table iii.sales_log add if not exists partition(PARTITION_wk="+week_number+") location '/iii-bigdata/data/"+week_number+"' ")
    
    #drop table first
    cur.execute("DROP TABLE iii.join_sales")

    #join table (table: sales_log and starbucks_drink_menu)
    cur.execute("CREATE TABLE iii.join_sales ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC AS SELECT number_id, sales_date, beverage_category, beverage, \
                                                                                                                   calories, partition_wk FROM iii.sales_log a \
                                                                                                            JOIN iii.starbucks_drink_menu b ON a.product_id = b.id ")

    #drop partition first
    cur.execute("ALTER TABLE iii.sales_analysis DROP IF EXISTS PARTITION (partition_wk="+week_number+") ")
    
    #insert into table sales_analysis with new partition
    cur.execute("INSERT INTO iii.sales_analysis PARTITION(partition_wk) SELECT number_id, sales_date, beverage_category, beverage, calories, partition_wk FROM iii.join_sales ")
    
    print("\nfinished with week of year:"+week_number)

except Exception as ex:
    print(ex)
finally:
    cur.close()
    conn.close()

