import com.mindtree.constants.Constants.JobName
import com.mindtree.jobs.DataIngestionJob
import com.mindtree.jobs.Main.spark
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class JobTest extends FunSuite{
  val spark = SparkSession.builder().appName(JobName).master("local")
    //.enableHiveSupport()
    .config("hive.exec.dynamic.partition","true")
    .config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate()


  test("Positive-Test-Case"){
    val ingestionJob = new DataIngestionJob(spark,"D:\\frameworks\\Web-Analytics-Data-Ingestion\\src\\test\\resources\\positive-test.txt",
      "C:/Users/M1032643/Documents/Spark 301/GeoIPCountryWhois.csv","C:/hive_storage")
    ingestionJob.process()
    val records = spark.read.parquet("D:\\frameworks\\Web-Analytics-Data-Ingestion\\data")
    assert(records.count()==2)
  }
  test("Negative-Test-Case"){
    val ingestionJob = new DataIngestionJob(spark,"D:\\frameworks\\Web-Analytics-Data-Ingestion\\src\\test\\resources\\negative-test.txt",
      "C:/Users/M1032643/Documents/Spark 301/GeoIPCountryWhois.csv","C:/hive_storage")
    ingestionJob.process()
    val records = spark.read.parquet("D:\\frameworks\\Web-Analytics-Data-Ingestion\\data")
    assert(records.count()!=2)
  }
}
