import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
      val config = new SparkConf().setAppName("ip_ana").setMaster("local[6]")
      val sc = new SparkContext(config)

      val result = sc.textFile("dataset/access_log_sample.txt")
        .map(item => (item.split(" ")(0), 1))
        .filter(item => StringUtils.isNotBlank(item._1))
        .reduceByKey((curr, agg) => curr + agg)
        .sortBy(item => item._2, false)
        .take(10)

      result.foreach(item => println(item))

    }
}
