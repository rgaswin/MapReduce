i1. Unzip the contents of the file 
2. Open a terminal or command prompt 
3. Navigate to the sourcecode folder in the assignment folder. 
4. There are 4 projects for this assignment. 
5. Get inside each project and open the make file in the project. 
6. Customize the top most section in the make file per your environment. 
   There will be a header that starts with "# Customize these paths for your environment.
# -----------------------------------------------------------"

Please configure these parameters per your environment 

hadoop.root=/users/aswingopalan/Downloads/hadoop-2.7.3
jar.name=MeanTemperature-1.0.jar
jar.path=target/${jar.name}
local.input=/users/aswingopalan/Desktop/Input
local.output=/users/aswingopalan/Desktop/Output
# AWS EMR Execution
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.bucket.name=rgaswinbucket
aws.subnet.id=subnet-9f622fb2
aws.input=input1
aws.output=output-simple-mean
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large

7. From the terminal, go to each project folder individually and execte "make upload-input-aws" command.
8. Once the input data is uploaded to s3, execute this command "make cloud"

9. The program should deploy and execute in aws successfully 


