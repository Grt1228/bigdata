import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object nuion {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("ip_ana").setMaster("local[6]")
    val sc = new SparkContext(config)
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    val rdd2 = sc.parallelize(Seq(4, 5, 6,6))
    val ints = rdd1.union(rdd2)
      .collect()

    ints.foreach(item => println(item))

  }
}
