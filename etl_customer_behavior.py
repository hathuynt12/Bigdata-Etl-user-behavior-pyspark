from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from pyspark.sql.functions import concat_ws
from datetime import datetime, timedelta
import os


spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores",8).getOrCreate()

def category_AppName(df): #chuẩn hóa hoặc phân loại AppName thành Type
    df=df.withColumn("Type",
                     when ((col("AppName").isin('CHANNEL','DSHD','KPLUS','KPlus')),"Truyen Hinh")
                    .when ((col("AppName").isin('VOD','FIMS_RES','BHD_RES','VOD_RES','FIMS','BHD','DANET')),"Phim Truyen")   
                    .when((col("AppName") == 'RELAX'), "Giai Tri")
                    .when((col("AppName") == 'CHILD'), "Thieu Nhi")
                    .when((col("AppName") == 'SPORT'), "The Thao")
                    .otherwise("Error")
                    )
    df=df.select('Contract','Type','TotalDuration')
    df=df.filter(df.Contract != '0')
    df=df.filter(df.Type !='Error')
    return df

def ETL_1_day(path,path_day):
    print('------------------------')
    print('Read data from Json file')
    print('------------------------')
    df=spark.read.json(path+path_day+".json")
    print('------------------------')
    print('Category AppName')
    print('------------------------') 
    df=df.select('_source.*')

    df=category_AppName(df)
    print('-----------------------------')
    print('Pivoting data')
    print('-----------------------------')
    df=df.groupBy("Contract").pivot("Type",["Truyen Hinh", "Phim Truyen", "Giai Tri", "Thieu Nhi", "The Thao"]).sum("TotalDuration")
    

    df=df.withColumn("Date",to_date(lit(path_day),'yyyyMMdd'))
    
    return df
def most_watch(df):
    df=df.withColumn("Mostwatch",greatest(col("Total_Truyen_Hinh"),col("Total_Phim_Truyen"), col("Total_Giai_Tri"), col("Total_Thieu_Nhi"), col("Total_The_Thao")))
    df=df.withColumn("Mostwatch",
                     when(col("Mostwatch")==col("Total_Truyen_Hinh"),"Truyen Hinh")
                     .when(col("Mostwatch")==col("Total_Phim_Truyen"),"Phim Truyen")
                     .when(col("Mostwatch")==col("Total_Giai_Tri"),"Giai Tri")
                     .when(col("Mostwatch")==col("Total_Thieu_Nhi"),"Thieu Nhi")
                     .when(col("Mostwatch")==col("Total_The_Thao"),"The Thao")
                     )
    return df
 #ghi đè lại cột MostWatch bằng logic mới trong lần thứ hai.
 
def customer_taste(df):
    df=df.withColumn("Taste",concat_ws('-',
                                       when( col("Total_Giai_Tri").isNotNull(),lit("Giai Tri")) 
                                       ,when(col("Total_Truyen_Hinh").isNotNull(),lit("Truyen Hinh"))
                                       ,when(col("Total_Phim_Truyen").isNotNull(),lit("Phim Truyen"))
                                       ,when(col("Total_Thieu_Nhi").isNotNull(),lit("Thieu Nhi"))
                                       ,when(col("Total_The_Thao").isNotNull(),lit("The Thao"))
                                        )
                    )
    return df
    # Vì concat_ws("-", ...) chỉ chấp nhận Column  → cần lit() để dùng giá trị chuỗi này như một cột.
def find_active(df):
    df=df.groupBy("Contract").agg(
        sf.sum("Giai Tri").alias("Total_Giai_Tri"),
        sf.sum("Phim Truyen").alias("Total_Phim_Truyen"),
        sf.sum("The Thao").alias("Total_The_Thao"),
        sf.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
        sf.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
        sf.countDistinct("Date").alias("Active")   
    )
    df = most_watch(df)
    df = customer_taste(df)
    df = df.withColumn("Level",when(col("Active")>=4,"High")
                       .otherwise("Low"))
    return df

def import_to_mysql(result):
    url = 'jdbc:mysql://' + {MYSQL_HOST} + ':' + {MYSQL_PORT} + '/'+'bigdata'
    driver = "com.mysql.cj.jdbc.Driver"
    user = {MYSQL_USER}
    password = {MYSQL_PASSWORD}
    result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','customer_content_stats').option('user',user).option('password',password).mode('append').save()
    return print("Data Import Successfully")

def main_task(path):
    
    start_date="20220401"
    end_date="20220407"
    date_list = pd.date_range(start= start_date ,end = end_date).strftime('%Y%m%d').tolist()
    print("ETL data file:"+date_list[0]+"json")
    result=ETL_1_day(path,date_list[0])
    
    for x in date_list[1:]:
        print("ETL data from "+x+".json")
        result = result.union(ETL_1_day(path,x))
    
    # Cache sau khi union toàn bộ dữ liệu
    result = result.cache()

    print('-----------------------------')
    print('Showing data')
    print('-----------------------------') 

    result.show(10)
    print('-----------------------------')

    # Tính chỉ số hoạt động
    result = find_active(result)
    print('-----------------------------')
    print('Final result after find_active')
    result.show(10)

     # Ghi vào MySQL
    import_to_mysql(result)
    print("Finished job")
    print("Số dòng cuối cùng:", result.count())
    return result

path = "E:\\bigdata\\Dataset\\log_content\\"
df=main_task(path)