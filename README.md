# POPI
Test code for Load-balancing Distributed Outer Joins through Operator Decomposition

1, Generate data sets and convert each tuple in the form of <key|value> pair, while key is the NATIONKEY and value are other attributes. The two relations should be uploaded to HDFS as two files.

2, Export jar file by the source codes.

3, With the jar file, we can submit jobs. There are two main input parameter for each algorithm, i.e., the input path for the relation R and S. It should be noted that there should be an additional parameter for POPI, i.e., sample rate. However, we have set it to 1 in our code. An sample of the job submission command is shown as below. There, we assign the test algorithm by "class", i.e., the example below shows the case for ROJA algorithm. The other parameters such as "spark://taurusi5033:7077" is the sparkcontext, "120G" is the execution memory, and "hdfs://taurusi5033:54310/input/R/" means the path for the uploaded R.

> spark-submit \ <br/>
  --class ROJA \ <br/>
  --master spark://taurusi5033:7077 \ <br/>
  --executor-memory 120G \ <br/>
  /home/lcheng/POPI/test.jar \ <br/>
  spark://taurusi5033:7077 \ <br/>
  hdfs://taurusi5033:54310/input/R/ \ <br/>
  hdfs://taurusi5033:54310/input/S/  <br/>

  
4, How to set Spark, please refer to http://spark.apache.org/.

5, If any questions, please email to long.cheng(AT)ucd.ie
