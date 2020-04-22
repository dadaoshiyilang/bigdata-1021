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
object RealTimeTopN {
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

    val datMapDS: DStream[(String, Int)] = dataDS.map {
      line => {
        val splits = line.split(",")
        val longTimes: Long = splits(0).toLong
        val dateTimes: Date = new Date(longTimes)
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dateStr: String = format.format(dateTimes)
        (dateStr +"_"+ splits(1)+"_" + splits(4), 1)
      }
    }

    // (2020-03-21华北5,4)
    // (2020-03-21华中3,2)
    // (2020-03-21华东2,5)
    // (2020-03-21华北1,5)
    val updateMapDs: DStream[(String, Int)] = datMapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )

    val cntMapDs: DStream[(String, (String, Int))] = updateMapDs.map {
      case (str, cnt) => {
        val ks = str.split("_")
        (ks(0) + "_" + ks(1), (ks(2), cnt))
      }
    }
    val groupByKeyDs: DStream[(String, Iterable[(String, Int)])] = cntMapDs.groupByKey()


    val takeDs: DStream[(String, List[(String, Int)])] = groupByKeyDs.mapValues(
      datas => {
        datas.toList.sortBy(-_._2)
      }.take(3)
    )
    takeDs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

