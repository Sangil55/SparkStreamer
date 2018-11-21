package com.kt.swinno
import org.apache.log4j.Logger


/**
 * @author daegi.jeong
 */
object App {
  val logger:Logger = Logger.getLogger(this.getClass.getName)
  def main(args : Array[String]) {

    /** 설정파일 읽기 & 설정 세팅 */
    val prop = Common.getProp //설정 파일 읽기
    val inputTimeFormat = prop.getProperty("inputTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS")
    val outputTimeFormat = prop.getProperty("outputTimeFormat", "HH:mm:ss.SSS")
    val outputMode = prop.getProperty("outputMode")
    logger.info("* inputTimeFormat : " + inputTimeFormat)
    logger.info("* outputTimeFormat : " + outputTimeFormat)
    logger.info("* output mode : " + outputMode)


    /** 스키마 설정 READ */
    //@TODO schema customization
    //val schema = LogMessage.loadSchema()
    val schema = LogMessage.logSchema;

    /**import spark sql functions*/
    logger.info("Import spark functions")
    val spark = KafkaManager.getSession()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**read data from kafka*/
    val ds = KafkaManager.read().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    //ds.printSchema()


    /** read stream data */
    //로그 데이터에서 "type"과 "time" 추출
    logger.info("Extract \"type\" and \"time\"")

    val logBulk = ds
      //.select($"key", split($"value", "\n").as("value"))
      .select(split($"value", "\n").as("value"))
      .withColumn("jsonString", explode($"value"))


    val logs=logBulk
      //.select(from_json($"value", LogMessage.logSchema).as("parsed_json")) //json string parsing
      //.select($"key", from_json($"jsonString", schema).as("parsed_json"))
      .select(from_json($"jsonString", schema).as("parsed_json"))
      .select(
        $"parsed_json.mas_host".as("mas_host"),
        $"parsed_json.type".as("type"),
        $"parsed_json.logid".as("logid"),
        to_timestamp($"parsed_json.time", inputTimeFormat).cast("timestamp").as("time")) // string -> timestamp 자료형 변환

    /**Log Aggregation*/
    val summary = logs
      .withWatermark("time", prop.getProperty("kafkaOutputWatermark","10 seconds"))
      //.dropDuplicates("logid")
      .groupBy($"mas_host",window($"time", "10 second"),$"type").count//.orderBy('window, 'type)
    // 연산 결과 예시
    // {"window":{"start":"2018-07-25T11:23:10.000+09:00","end":"2018-07-25T11:23:20.000+09:00"},"type":"xml-MAS-res_ping","count":1}

    /** 출력 형식에 맞게 수정 */
    val result = summary.select(
      date_format($"window.start", outputTimeFormat).as("time"),
      $"mas_host",
      $"type",
      $"count"
      //,date_format(current_timestamp(),outputTimeFormat).as("processed_time")
    )

    /**Export result  */
    logger.debug("Export data to kafka")
    if(outputMode.equalsIgnoreCase("complete")){
      logger.debug("write data with complete mode")
      KafkaManager.writeKafkaWithCompleteMode(result.toDF())
    }else if(outputMode.equalsIgnoreCase("append")){
      logger.debug("write data with append mode")
      KafkaManager.writeKafkaWithAppendMode(result.toDF())
      //KafkaManager.writeFile(result.toDF())
    }else if(outputMode.equalsIgnoreCase("update")){
      logger.debug("write data with update mode")
      KafkaManager.writeKafkaWithUpdateMode(result.toDF())
    }
    //KafkaManager.writeKafka(result.toDF()) //kafka 전송
  }

}
