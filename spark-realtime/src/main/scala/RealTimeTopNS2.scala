import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 用于测试从Kafka中消费数据
  */
object RealTimeTopNS2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点  用于保存状态
    ssc.checkpoint("D:\\dev\\workspace\\bigdata-1021\\spark-1021\\cp")

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "ads-1021"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建DS (null,1584795964275,华北,北京,101,4)
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))

    val dataDS: DStream[String] = kafkaDS.map(_._2)

    val dataWins: DStream[String] = dataDS.window(Seconds(12), Seconds(3))


    val datMapDS: DStream[((String, String), Int)] = dataWins.map {
      line => {
        val splits = line.split(",")
        val longTimes: Long = splits(0).toLong
        val dateTimes: Date = new Date(longTimes)
        val format: SimpleDateFormat = new SimpleDateFormat("mm:ss")
        val dateStr: String = format.format(dateTimes)
        ((splits(4), dateStr), 1)
      }
    }
    val reduceDs: DStream[((String, String), Int)] = datMapDS.reduceByKey(_+_)
    reduceDs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

