import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Main {

  def main(args : Array[String])  {
    val session = SparkSession
                  .builder()
                  .appName("test-sink")
                  .master("local[2]")
                  .getOrCreate()
  }

}
