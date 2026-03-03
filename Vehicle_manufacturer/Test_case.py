from email import header
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,sum
import os


class sampleTest(unittest.TestCase):
    sample_score = {"result_1":0,
                    "result_2":0,
                    "result_3":0}
    @classmethod
    def setUpClass(cls):
        """
        Start Spark, define config and path to test 
        """
        
        cls.spark=SparkSession \
            .builder \
            .appName("sampleTest") \
            .master("local") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    def test_result_1(self):
        self.sample("result_1",8000)
    
    def test_result_2(self):
        self.sample("result_2",8000)

    def test_result_3(self):
        self.sample("result_3",440)

    def sample(self,file_name,expected_count):
        input_df = self.read_file(file_name)
        expected_headers = {"result_1" : ["County","City","State","Model Year","Make","Model","Electric Vehicle Type","Clean Alternative Fuel Vehicle (CAFV) Eligibility","Electric Range","DOL Vehicle ID"],
                            "result_2" : ["County","City","State","Model Year","Make","Model","Electric Vehicle Type","Electric Range","Comment"],
                            "result_3" : ["Make","County","Total_count"]                                        
                            }
        if input_df != None:
            header_check = 0
            actual_count = input_df.count()
            cnt = 0
            if input_df.columns == expected_headers[file_name]:
                header_check += 10
            else:
                print("header does not match for the file: %s",file_name)

            self.assertEqual(actual_count,expected_count,"Count of records does not match")
            if actual_count == expected_count:
                cnt += 90
            tot = header_check + cnt
            self.sample_score[file_name] = tot
        else:
            self.fail("The required input file seems missing")

    def read_file(self,file_name):
        try:
            cwd = os.getcwd()
            path = "file://" + cwd + "/output/" + file_name
            df = self.spark.read.csv(path,header=True)
            return df

        except:
            print("----------------------------------------------------------------------------------------------------------")
            print("Looks like the output directory for following file is missing.{}".format(file_name))
            print("Please check whether the output file directory name matches the directory name given in instructions.")
            print("Note that you can still go ahead and submit your test. Scoring will happen accordingly.")
            print("----------------------------------------------------------------------------------------------------------")

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark
        """
        print("          ")
        print(cls.sample_score)
        print("         ")
        test_score = 0
        for i,j in cls.sample_score.items():
            test_score = test_score+(j/3)
        print("**********************************************************")
        print("Sample_Score:",test_score)
        print("               ")
        print("NOTE: The sample score does not represent the Final Score.")
        print("**********************************************************")
        cls.spark.stop()
        
    
    
if __name__ == "__main__":
    unittest.main()




