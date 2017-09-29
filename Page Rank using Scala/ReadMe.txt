README : 

1) Extract the contents of the zip file
2) Execute the command "make alone" after changing the input and output file path in the makefile (local.input and local.output).
3) Execute the command "make cloud" after changing the input and output file path in the makefile under AWS section (aws.input and aws.output).
4) Also, when you are running the program in AWS, set master to "yarn" in the first line of main function in App.scala. 

   val conf = new SparkConf().setAppName("Page Rank").setMaster("yarn")


Also please set the following variables as per your environment for AWS execution : 
aws.region
aws.bucket.name
aws.subnet.id

