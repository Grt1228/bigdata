import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}


object TaxiAnalysisRunner {


  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    //2. 导入函数或者隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //3. 读取文件
    val taxiRaw = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")
    taxiRaw.show()
    taxiRaw.printSchema()

    //// 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(parse)
    //val trips = taxiParsed.take(10)
    //taxiParsed.foreach(item => println(item))
    val taxiGood = taxiParsed.toDS()

    // 5. 过滤行程无效的数据
    //5.绘制时长直方图
    //5.1编写UDF 完成时长计算，将毫秒转为小时单位
    val minutes = (pickUpTime: Long, dropOffTime: Long) => {
      val duration = dropOffTime - pickUpTime
      val minutes = TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS)
      minutes
    }
    val hoursUDF = udf(minutes)

    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime").as("duration"))
      .count()
      .sort("duration")
      .show()

    spark.udf.register("minutes", minutes)
    val taxiClean = taxiGood.where("minutes(pickUpTime, dropOffTime) BETWEEN 0 AND 3")
    taxiClean.show()

  }


  /**
   * 将 Row 对象转为 Trip 对象, 从而将 DataFrame 转为 Dataset[Trip] 方便后续操作
   *
   * @param row DataFrame 中的 Row 对象
   * @return 代表数据集中一条记录的 Trip 对象
   */
  def parse(row: Row): Trip = {
    // 通过使用转换方法依次转换各个字段数据
    val richRow = new RichRow(row)
    val license = richRow.getAs[String]("hack_license").orNull
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")

    // 创建 Trip 对象返回
    Trip(license, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }

  /**
   * 将时间类型数据转为时间戳, 方便后续的处理
   * @param row 行数据, 类型为 RichRow, 以便于处理空值
   * @param field 要处理的时间字段所在的位置
   * @return 返回 Long 型的时间戳
   */
  def parseTime(row: RichRow, field: String): Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)

    val timeOption = row.getAs[String](field)
    timeOption.map( time => formatter.parse(time).getTime )
      .getOrElse(0L)
  }

  /**
   * 将字符串标识的 Double 数据转为 Double 类型对象
   * @param row 行数据, 类型为 RichRow, 以便于处理空值
   * @param field 要处理的 Double 字段所在的位置
   * @return 返回 Double 型的时间戳
   */
  def parseLocation(row: RichRow, field: String): Double = {
    row.getAs[String](field).map( loc => loc.toDouble ).getOrElse(0.0D)
  }


}

class RichRow(row: Row) {

  def getAs[T](field: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(field)) || StringUtils.isBlank(row.getAs[String](field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }
  }
}

/**
 * 代表一个行程, 是集合中的一条记录
 *
 * @param license     出租车执照号
 * @param pickUpTime  上车时间
 * @param dropOffTime 下车时间
 * @param pickUpX     上车地点的经度
 * @param pickUpY     上车地点的纬度
 * @param dropOffX    下车地点的经度
 * @param dropOffY    下车地点的纬度
 */
case class Trip(
                 license: String,
                 pickUpTime: Long,
                 dropOffTime: Long,
                 pickUpX: Double,
                 pickUpY: Double,
                 dropOffX: Double,
                 dropOffY: Double
               )


