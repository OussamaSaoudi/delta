package io.delta
import io.delta.kernel.{types => KernelTypes}
import io.delta.kernel.types.{BooleanType => KernelBooleanType, DataType => KernelDataType, IntegerType => KernelIntegerType, LongType => KernelLongType, StringType => KernelStringType, StructField => KernelStructField, StructType => KernelStructType}
import org.apache.spark.sql.types.{BooleanType => SparkBooleanType, DataType => SparkDataType, IntegerType => SparkIntegerType, LongType => SparkLongType, StringType => SparkStringType, StructField => SparkStructField, StructType => SparkStructType}
import org.apache.spark.sql.{types => SparkType}
import scala.collection.JavaConverters._

object SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  def convertKernelSchemaToSparkSchema(kernelSchema: KernelStructType): SparkStructType = {
    SparkStructType(kernelSchema.fields().asScala.map { field =>
      SparkStructField(
        field.getName,
        convertKernelDataTypeToSparkDataType(field.getDataType),
        field.isNullable)
    })
  }

  def convertKernelDataTypeToSparkDataType(kernelDataType: KernelDataType): SparkDataType = {
    kernelDataType match {
      case _: KernelStringType => SparkStringType
      case _: KernelBooleanType => SparkBooleanType
      case _: KernelIntegerType => SparkIntegerType
      case _: KernelLongType => SparkLongType
      case _: KernelTypes.TimestampType => SparkType.TimestampType
      case _: KernelTypes.FloatType => SparkType.FloatType
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  def convertSparkSchemaToKernelSchema(sparkSchema: SparkStructType): KernelStructType = {
    new KernelStructType(
      sparkSchema.fields
        .map { field =>
          new KernelStructField(
            field.name,
            convertSparkDataTypeToKernelDataType(field.dataType),
            field.nullable)
        }
        .toList
        .asJava)
  }

  def convertSparkDataTypeToKernelDataType(sparkDataType: SparkDataType): KernelDataType = {
    sparkDataType match {
      case SparkStringType => KernelStringType.STRING
      case SparkBooleanType => KernelBooleanType.BOOLEAN
      case SparkIntegerType => KernelIntegerType.INTEGER
      case SparkLongType => KernelLongType.LONG
      case SparkType.TimestampType => KernelTypes.TimestampType.TIMESTAMP
      case SparkType.FloatType => KernelTypes.FloatType.FLOAT
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

}
