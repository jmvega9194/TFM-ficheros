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
FuenteVentas_node1743868867922 = glueContext.create_dynamic_frame.from_catalog(database="jmvegadb1", table_name="ventasfashionator9000_csv", transformation_ctx="FuenteVentas_node1743868867922")

# Script generated for node totalxcliente
totalxcliente_node1743868975233 = sparkAggregate(glueContext, parentFrame = FuenteVentas_node1743868867922, groups = ["cliente_id"], aggs = [["total_eur", "sum"]], transformation_ctx = "totalxcliente_node1743868975233")

# Script generated for node SQL Query
SqlQuery5845 = '''
SELECT cliente_id, `sum(total_eur)`
FROM top10
ORDER BY `sum(total_eur)` DESC
LIMIT 10
'''
SQLQuery_node1743870248581 = sparkSqlQuery(glueContext, query = SqlQuery5845, mapping = {"top10":totalxcliente_node1743868975233}, transformation_ctx = "SQLQuery_node1743870248581")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743870248581, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743868836778", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1743871155409 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1743870248581, connection_type="s3", format="csv", connection_options={"path": "s3://jmvegafashionator9000/datostransformados/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1743871155409")

job.commit()