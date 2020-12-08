import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test
case class Person(name: String, age: Int)
case class Weibo(phone: Long, uid: Long)

class Intro {

  @Test
  def dsIntro1(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.textFile("dataset/b.txt")
    val lines = sourceRDD.map(_.split("\t"))
    val allWeibo = lines.map(x => Weibo(x(0).toLong,x(1).toLong))
    val personDS: Dataset[Weibo] = allWeibo.toDS()

    val resultDS = personDS.where( 'phone >= 150 )
      .where( 'uid <= 234 )
      .select( 'phone )
      .as[Long]
        .sort('phone)

    resultDS.show()
  }

  @Test
  def dsIntro2(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    val personDS: Dataset[Person] = sourceRDD.toDS()

    val resultDS = personDS.where( 'age > 10 )
      .where( 'age < 20 )
      .select( 'name )
      .as[String]

    resultDS.show()
  }
}
