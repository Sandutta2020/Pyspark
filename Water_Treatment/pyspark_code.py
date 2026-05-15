 # -*- coding: utf-8 -*-
import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, TimestampType
dirname ="/Volumes/wt_catalog/wt_schema/wt_vol"

def read_data (spark,input_file,Schema):
    ''' 
    spark_session : spark 
    for input_file : input_file 
    for schema : Schema
    '''

    df = spark.read.csv(input_file,header=False,schema=Schema)
            
    # Write your code
   
    return df  #return the final dataframe


def clean_data(input_df):
    '''
    for input file: input_df # The Final dataframe of the read_data function.
    '''

    df = input_df
    df=df.drop_duplicates(subset=["ph","Hardness","Solids","Chloramines","Sulfate","Conductivity","Organic_carbon","Trihalomethanes","Turbidity","Potability"])
    df=df.fillna(0.0)
    #print(df.show())
       #Write your code 
    
    return df  #return the final dataframe

def TDS_reviewed(input_df):
    '''
    for input file: input_df // The final dataframe of clean_data funtion.
    '''
    print("-------------------------")
    print("Starting TDS_reviewed")
    print("-------------------------")

    df = input_df
    #Write your code
    windows_spec=Window.partitionBy(lit(" ")).orderBy(lit(" "))
    df=df.withColumn("Sl_No",row_number().over(windows_spec))

    df=df.withColumn("TDS_review",when(col("Hardness") <= 170.0,'Ideal').\
        when((col("Hardness") > 170.0) & (col("Hardness") < 500.0),'Recommended').\
                    otherwise('Unacceptable'))
    df=df.drop_duplicates(subset=["ph","Hardness","Solids","Chloramines","Sulfate","Conductivity","Organic_carbon","Trihalomethanes","Turbidity","Potability","TDS_review"])
    df=df.orderBy("Sl_No")
    df=df.select("Sl_No","ph","Hardness","Solids","Chloramines","Sulfate","Conductivity","Organic_carbon","Trihalomethanes","Turbidity","Potability","TDS_review")
    print(df.show(3))
    return df     #return the final dataframe


def Drinking_Water(input_df):
    '''
    for input file: input_df // The final dataframe of Summary_Report funtion.
    '''
    print("-------------------------")
    print("Starting Drinking_Water")
    print("-------------------------")
  
    df = input_df   
    df =df.drop("Sl_No","Potability","Solids")
    df=df.withColumnRenamed("Hardness","Hardness(ppm)")
    df=df.withColumnRenamed("ph","ph".upper()).\
            withColumnRenamed("Hardness(ppm)","Hardness(ppm)".upper()).\
                withColumnRenamed("Chloramines","Chloramines".upper()).\
                    withColumnRenamed("Sulfate","Sulfate".upper()).\
                        withColumnRenamed("Conductivity","Conductivity".upper()).\
                            withColumnRenamed("Organic_carbon","Organic_carbon".upper()).\
                                withColumnRenamed("Trihalomethanes","Trihalomethanes".upper()).\
                                    withColumnRenamed("Turbidity","Turbidity".upper()).\
                                        withColumnRenamed("TDS_review","TDS_review".upper())
    df=df.filter((col("PH") >= 6.5) & (col("PH") <= 8.5)).filter(col("SULFATE") < 500.0)
    windows_spec=Window.partitionBy(lit(" ")).orderBy(lit(" "))
    df=df.withColumn("Sl_No",row_number().over(windows_spec))
    print(df.printSchema())
    df=df.select("Sl_No","PH","HARDNESS(PPM)","CHLORAMINES","SULFATE","CONDUCTIVITY","ORGANIC_CARBON","TRIHALOMETHANES","TURBIDITY","TDS_REVIEW")
    print(df.show(2))
    #Write your code
    
    
    
    return df     #return the final dataframe


def load_data(data,output_path):
    '''
    The following are the parameters :

        data : All the tasks output data frame.

        outputpath: Location where the file needs to be saved 

    '''

    if (data.count() != 0):
        print("Loading the data",output_path)
        data.coalesce(1).write.mode("overwrite").csv(output_path,header=True)

        #Write your code above this line
    else:
        print("Empty dataframe, hence cannot save the data",output_path)



def main():

    """ Main driver program to control the flow of execution.
        Please DO NOT change anything here.
    """
    #Clean the output files for fresh execution
    outputfile_cleanup()
    #Get a new spark session
    spark = (SparkSession.builder.\
                          appName("Water Data Analysis").\
                          getOrCreate())
    #spark.sparkContext.setLogLevel("ERROR")



    Schema = StructType([ \
    StructField("ph",DoubleType(),True),\
    StructField("Hardness",DoubleType(),True), \
    StructField("Solids",DoubleType(),True), \
    StructField("Chloramines",DoubleType(),True), \
    StructField("Sulfate",DoubleType(),True),\
    StructField("Conductivity",DoubleType(),True),\
    StructField("Organic_carbon",DoubleType(),True), 
    StructField("Trihalomethanes",DoubleType(),True),\
    StructField("Turbidity",DoubleType(),True),\
    StructField("Potability",DoubleType(),True)
    ])

    #cwd = os.getcwd()
    dirname = "/Volumes/wt_catalog/wt_schema/wt_vol"
    input_file =  dirname + "/inputfile/water_sample.csv"
    output_path = dirname + "/output"
    result_1_path = output_path + "/TDS_reviewed"
    result_2_path = output_path + "/Drinking_Water"
   

    try:
        task_1 = read_data(spark,input_file,Schema)
    except Exception as e:
        print("Getting error in the read_data function",e)
    try:
        task_2 = clean_data(task_1)
    except Exception as e:
        print("Getting error in the task_2 function",e)
    try:
        task_3 = TDS_reviewed(task_2)
    except Exception as e:
        print("Getting error in the task_3 function",e)
    try:
        task_4 = Drinking_Water(task_3)
    except Exception as e:
        print("Getting error in the task_4 function",e)
        

    try:
        load_data(task_3,result_1_path)
    except Exception as e:
        print("Getting error while loading TDS_reviewed",e)
    try:
        load_data(task_4,result_2_path)
    except Exception as e:
        print("Getting error while loading Drinking_Water ",e)


    spark.stop()

def outputfile_cleanup():

    """ Clean up the output files for a fresh execution.
        This is executed every time a job is run. 
        Please DO NOT change anything here.
    """

    #cwd = os.getcwd()
    #dirname = os.path.dirname(cwd)
    dirname = "/Volumes/wt_catalog/wt_schema/wt_vol"
    path = dirname + "/output/"
    if (os.path.isdir(path)):
        try:
            shutil.rmtree(path)  
            print("% s removed successfully" % path)
            os.mkdir(path)  
        except OSError as error:  
            print(error)  
    else:
        print("The directory does not exist. Creating..% s", path)
        os.mkdir(path)

if __name__ == "__main__":
	main()