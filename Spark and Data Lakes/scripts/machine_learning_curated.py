import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer trusted
StepTrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ch-udacity-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainertrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1679564480158 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ch-udacity-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1679564480158",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=StepTrainertrusted_node1,
    frame2=AccelerometerTrusted_node1679564480158,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1679565612432 = DropFields.apply(
    frame=Join_node2, paths=["user"], transformation_ctx="DropFields_node1679565612432"
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1679565612432,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ch-udacity-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
