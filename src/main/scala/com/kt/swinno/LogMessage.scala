package com.kt.swinno
import scala.util.parsing.json._
import com.kt.swinno.Common.prop
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

import scala.collection._
import scala.io.Source

object LogMessage {
  val logger:Logger = Logger.getLogger(this.getClass.getName)
  /*
     +-------------+-----------------+------------------+
     | field name  |  StructType     |                  |
     +-------------+-----------------+------------------+
     | result      | String          |                  |
     | jobid       | String          |                  |
     | host        | String          |                  |
     | logid       | String          |                  |
     | time        | String          | HH.mm.ss.SSS     |
     | c_msg_id    | String          |                  |
     | type        | String          |                  |
     +-------------+-----------------+------------------+
  */

  @deprecated //미리 정의된 로그 스키마
  val logSchema = StructType(Seq(
    StructField("result", StringType, true),
    StructField("jobid", StringType, true),
    StructField("host", StringType, true),
    StructField("mas_host", StringType, true),
    StructField("logid", StringType, true),
    StructField("time", StringType, true),
    StructField("c_msg_id", StringType, true),
    StructField("type", StringType, true)
  ))

  /**
    * json 문자열에서 structfield 추출.
    * @param line
    * @return
    */
  def addStructField(line:String):StructField = {
    def checkNameValue(o:Option[String]):String ={
      o match {
        case Some(s:String) => s
      }
    }

    def checkTypeValue(o:Option[String])={
      o match {
        case Some(s:String) => getType(s)
      }
    }
    def checkNullableValue(o:Option[String])={
      o match {
        case Some(s:String) => getNullable(s)
      }
    }
    def getType(s:String) ={
      s match {
        case "StringType" => StringType
        case "IntegerType" => IntegerType
        case "LongType" => LongType
        case "BooleanType" => BooleanType
        //@TODO 기타 타입 추가.
      }
    }
    def getNullable(s:String) ={
      s match{
        case "true" => true
        case "false" => false
      }
    }

    val json = JSON.parseFull(line)
    val map:Map[String, String] = json.get.asInstanceOf[Map[String,String]]
    val field:StructField = new StructField(
      checkNameValue(map.get("name")),
      checkTypeValue(map.get("type")),
      checkNullableValue(map.get("nullable")))
    field
  }


  /**
    * 입력데이터를 파싱할 스키마를 json파일로 부터 읽어들임.
    * @return
    */
  def loadSchema():StructType ={
    val path = new java.io.File(".").getCanonicalPath
    val file = prop.getProperty("schemaFileName", "schema.json")
    var customLogSchema:StructType = new StructType()
    logger.info("path : "+path);

    try{
      //
      val lines = Source
        .fromFile(path+"/resources/"+file)
        .getLines
        .toList
        .filterNot(line => line.startsWith("#"))

      val field = lines.map( x => addStructField(x))
      field.foreach(println(_))

      customLogSchema = StructType(field)
      customLogSchema.printTreeString()
      /*
      root
       |-- result: string (nullable = true)
       |-- jobid: string (nullable = true)
       |-- host: string (nullable = true)
       |-- time: string (nullable = true)
       |-- c_msg_id: string (nullable = true)
       |-- type: string (nullable = true)
       */

      logger.info("read Schema file done")
    }catch {
      case e:Exception =>  logger.error("read Schema failed : "+e.getMessage)
    }
    customLogSchema
  }
}
