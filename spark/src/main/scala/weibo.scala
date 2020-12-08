import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object weibo {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("ip_ana").setMaster("local[6]")
    val sc = new SparkContext(config)

    val result = sc.textFile("dataset/1.txt")
      .map(item => item.split("\t")(0))
      .filter(item => StringUtils.isNotBlank(item))
      .sortBy(item => item, false)
      .take(100000)
    result.foreach(item => println(item))

  }
}
