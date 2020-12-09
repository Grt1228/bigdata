import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

/**
  * MySQL 的访问方式有两种: 使用本地运行, 提交到集群中运行
  *
  * 写入 MySQL 数据时, 使用本地运行, 读取的时候使用集群运行
  */
object MySQLWrite {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("mysql write")
      .getOrCreate()

//     2. 读取数据创建 DataFrame
//        1. 拷贝文件
//        2. 读取
    val schema = StructType(
          List(
              StructField("name", StringType),
            StructField("age", IntegerType),
            StructField("gpa", FloatType)
          )
        )

        val df = spark.read
          .schema(schema)
          .option("delimiter", "\t")
          .csv("dataset/studenttab10k")

        // 3. 处理数据
        val resultDF = df.where("age < 30")

        // 4. 落地数据
        resultDF.write
          .format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/pr")
          .option("dbtable", "stu")
          .option("user", "root")
          .option("password", "123456")
          .option("driver","com.mysql.jdbc.Driver")
          .mode(SaveMode.Overwrite)
          .save()

//    spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://127.0.0.1:3306/pr")
//      .option("dbtable", "drink")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("driver","com.mysql.jdbc.Driver")
//      .load()
//      .show(10000)
  }

}
