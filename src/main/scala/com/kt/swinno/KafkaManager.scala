package com.kt.swinno
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object KafkaManager {
  val prop = readProp()
  val spark: SparkSession = connect() ///spark session

  private def readProp():Properties ={
    val prop = Common.readProperties()
    prop
  }

  def getSession():SparkSession ={
    spark
  }


  /**
    * spark session 생성
    * @return spark session
    */
  def connect(): SparkSession ={
    val conf = new SparkConf();
    conf.setMaster(prop.getProperty("sparkMaster", "local[*]"))
      .setAppName(prop.getProperty("sparkAppName", "messageSummarizer"))
      .set("spark.cores.max", prop.getProperty("sparkCoreMax", "10"))
      .set("spark.sql.shuffle.partitions", prop.getProperty("sparkSqlShufflePartitions","200"))
      .set("spark.executor.memory",prop.getProperty("sparkExecutorMemory","1g"))
      .set("spark.executor.cores",prop.getProperty("sparkExecutorCores","1"))

    SparkSession.builder()
     .config(conf)
     .getOrCreate()
  }

  /**
    * 토픽에서 스트림 읽어들임
    */
  def read(): DataFrame ={
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("subscribe", prop.getProperty("kafkaInputTopic", "myOutput"))
      //.option("kafka.auto.offset.reset", "latest")
      //.option("startingoffsets", prop.getProperty("startingoffsets", "earliest"))
      .option("failOnDataLoss", "false")
      .load()

    stream
  }

  /**
    * Kafka Topic으로 결과 내보내기
    * @param data 카프카에 저장할 Data
    */
  def writeKafka(data:DataFrame):Unit ={
    val query = data
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(OutputMode.Complete())
      //.outputMode(OutputMode.Append()  )
      .format("kafka")
      .trigger(Trigger.ProcessingTime(prop.getProperty("sparkTriggerTime","5 seconds")))
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("topic", prop.getProperty("kafkaOutputTopic", "mySummary"))
      .option("checkpointLocation","checkpoint")
      .start()

    query.awaitTermination()
  }


  //@TODO 함수 수정해야함.
  def writeKafkaWithCompleteMode(data:DataFrame):Unit ={
    val query = data
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(OutputMode.Complete)
      //.outputMode(OutputMode.Append()  )
      .format("kafka")
      .trigger(Trigger.ProcessingTime(prop.getProperty("sparkTriggerTime", "5 seconds")))
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("topic", prop.getProperty("kafkaOutputTopic", "mySummary"))
      .option("checkpointLocation","checkpoint")
      .start()

    query.awaitTermination()
  }

  def writeFile(data: DataFrame): Unit = {
    val query = data
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(OutputMode.Append)
      .format("json")
      .option("path",prop.getProperty("fileOutputPath","/tmp/sparkstreaming"))
      .option("checkpointLocation","checkpoint")
      .start
  }
  //@TODO 함수 수정해야함.
  def writeKafkaWithAppendMode(data:DataFrame):Unit ={
    val query = data
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(OutputMode.Append)
      //.outputMode(OutputMode.Append()  )
      .format("kafka")
      .trigger(Trigger.ProcessingTime(prop.getProperty("sparkTriggerTime", "5 seconds")))
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("topic", prop.getProperty("kafkaOutputTopic", "mySummary"))
      .option("checkpointLocation","checkpoint")
      .start()

    query.awaitTermination()
  }
  //@TODO 함수 수정해야함.
  def writeKafkaWithUpdateMode(data:DataFrame):Unit ={
    val query = data
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode(OutputMode.Update)
      .format("kafka")
      .trigger(Trigger.ProcessingTime(prop.getProperty("sparkTriggerTime","5 seconds")))
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("topic", prop.getProperty("kafkaOutputTopic", "mySummary"))
      .option("checkpointLocation","checkpoint")
      .start()

    query.awaitTermination()
  }
  def writeKafkaWithOutputMode(data:DataFrame, outputMode: OutputMode = OutputMode.Append):Unit ={
    val query = data.toJSON.toDF()
      .writeStream
      .outputMode(outputMode)
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafkaBootstrapServers", "localhost:9092"))
      .option("topic", prop.getProperty("kafkaOutputTopic", "mySummary"))
      .option("checkpointLocation","checkpoint")
      .start()

    query.awaitTermination()
  }

  def writeConsole(data:DataFrame): Unit ={
    val query = data.writeStream
      //.outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }

}
