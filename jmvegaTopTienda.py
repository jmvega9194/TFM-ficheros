import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node FuenteVentas
FuenteVentas_node1744060771218 = glueContext.create_dynamic_frame.from_catalog(database="jmvegadb1", table_name="ventasfashionator9000_csv", transformation_ctx="FuenteVentas_node1744060771218")

# Script generated for node totalxtienda
totalxtienda_node1744060830447 = sparkAggregate(glueContext, parentFrame = FuenteVentas_node1744060771218, groups = ["tienda"], aggs = [["total_eur", "sum"]], transformation_ctx = "totalxtienda_node1744060830447")

# Script generated for node Top10
SqlQuery6064 = '''
SELECT tienda, `sum(total_eur)`
FROM top10
ORDER BY `sum(total_eur)` DESC
LIMIT 10
'''
Top10_node1744060954807 = sparkSqlQuery(glueContext, query = SqlQuery6064, mapping = {"top10":totalxtienda_node1744060830447}, transformation_ctx = "Top10_node1744060954807")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Top10_node1744060954807, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744060733702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1744061122966 = glueContext.write_dynamic_frame.from_options(frame=Top10_node1744060954807, connection_type="s3", format="csv", connection_options={"path": "s3://jmvegafashionator9000/datostransformados/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744061122966")

job.commit()