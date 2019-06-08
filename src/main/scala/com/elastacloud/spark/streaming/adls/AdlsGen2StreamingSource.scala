package com.elastacloud.spark.streaming.adls

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class AdlsGen2StreamingSource extends DataSourceV2 with StreamWriteSupport with DataSourceRegister {
  override def shortName(): String = "adls-gen2-streaming"

  override def createStreamWriter(s: String,
                                  structType: StructType,
                                  outputMode: OutputMode,
                                  dataSourceOptions: DataSourceOptions): StreamWriter = {
    val options = AdlsGen2Options.fromDataSourceOptions(dataSourceOptions)
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    require(session.isDefined)

    if (outputMode != OutputMode.Append()) {
      throw new IllegalStateException("Adls Gen 2 streaming only supports output with Append mode")
    }
  }
}
