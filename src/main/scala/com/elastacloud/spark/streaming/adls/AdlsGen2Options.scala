package com.elastacloud.spark.streaming.adls

import org.apache.spark.sql.sources.v2.DataSourceOptions

private class AdlsGen2Options(val storageAccountName: String, val filesystem: String, val path: String) {}

object AdlsGen2Options {
  val STORAGE_ACCOUNT_NAME = "storage.name"
  val FILE_SYSTEM = "filesystem"
  val PATH = "path"

  def fromDataSourceOptions(dataSourceOptions: DataSourceOptions): AdlsGen2Options = {
    val storageAccountName = dataSourceOptions.get(STORAGE_ACCOUNT_NAME)
    if (!storageAccountName.isPresent) {
      throw new IllegalArgumentException(f"Storage account name '$STORAGE_ACCOUNT_NAME' must be provided")
    }

    val filesystem = dataSourceOptions.get(FILE_SYSTEM)
    if (!filesystem.isPresent) {
      throw new IllegalArgumentException(f"Filesystem name '$FILE_SYSTEM' must be provided")
    }

    val path = dataSourceOptions.get(PATH)
    if (!path.isPresent) {
      throw new IllegalArgumentException(f"Path value '$PATH' must be provided")
    }

    new AdlsGen2Options(storageAccountName.get(), filesystem.get(), path.get())
  }
}
