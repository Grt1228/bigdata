import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, SparkSession}
import org.junit.Test
case class Person(name: String, age: Int)
case class People(name: String, age: Int)
case class Weibo(phone: Long, uid: Long)
case class Emp(empno:Int,ename:String,job:String,mgr:Int,hiredate:String,sal:Int,comm:Int,deptno:Int)

class Intro {

  @Test
  def dsIntro1(): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.textFile("dataset/b.txt")
    val lines = sourceRDD.map(item => Array(item.substring(0,11), item.substring(12)))
    val allWeibo = lines.map(x => Weibo(x(0).toLong,x(1).toLong))
    val personDS: Dataset[Weibo] = allWeibo.toDS()

    val resultDS = personDS.where( 'phone >= 18500041993l )
      .where( 'uid <= 6774271247l )
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

  @Test
  def dsIntro3(): Unit = {
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val peopleRDD: RDD[People] = spark.sparkContext.parallelize(Seq(People("zhangsan", 9), People("lisi", 15)))
    val peopleDS: Dataset[People] = peopleRDD.toDS()
    peopleDS.createOrReplaceTempView("people")

    val teenagers: DataFrame = spark.sql("select name from people where age > 10 and age < 20")
    teenagers.show()
  }


  @Test
  def dsIntro4(): Unit = {
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val peopleDF: DataFrame = Seq(People("zhangsan", 15), People("lisi", 15)).toDF()

    /*
    +---+-----+
    |age|count|
    +---+-----+
    | 15|    2|
    +---+-----+
     */
    peopleDF.groupBy('age)
      .count()
      .show()
  }

  @Test
  def dsIntro5(): Unit = {
    import org.apache.spark.sql.SparkSession
        import org.apache.spark.sql.DataFrame

        val spark: SparkSession = new sql.SparkSession.Builder()
          .appName("hello")
          .master("local[6]")
          .getOrCreate()


        // 使用 load 方法
        val fromLoad: DataFrame = spark
          .read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load("dataset/BeijingPM20100101_20151231.csv")

        // Using format-specific load operator
        val fromCSV: DataFrame = spark
          .read
          .option("header", true)
          .option("inferSchema", true)
          .csv("dataset/BeijingPM20100101_20151231.csv")
        val writer: DataFrameWriter[Row] = fromCSV.write
        writer.text("file:///D:/dataset/beijingPM")

        // 使用 json 保存, 因为方法是 json, 所以隐含的 format 是 json
        //writer.json("file:///D:/dataset/beijingPM1.txt")

//    val spark: SparkSession = new sql.SparkSession.Builder()
//      .appName("hello")
//      .master("local[6]")
//      .getOrCreate()
//
//    val dfFromParquet = spark.read.load("dataset/beijing_pm")
//
//    // 将 DataFrame 保存为 JSON 格式的文件
//    dfFromParquet.repartition(1)
//      .write.format("json")
//      .save("dataset/beijing_pm_json")
  }

  @Test
  def dsIntro6(): Unit = {
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("dataset/BeijingPM20100101_20151231.csv")


    df.show(10)
    df.printSchema()


    df.createOrReplaceTempView("temp_table")

    spark.sql("select year, month, count(*) from temp_table where PM_Dongsi != 'NA' group by year, month")
      .show()
  }

  @Test
  def dsIntro7(): Unit = {
//    val spark: SparkSession = new sql.SparkSession.Builder()
//      .appName("hello")
//      .master("local[6]")
//      .getOrCreate()
//    case class MyData(a:Int,b:String)
//    import spark.implicits._
//
//    val ds = Seq(MyData(1, "Tom"), MyData(2, "Mary")).toDS
//    ds.show
  }
}
