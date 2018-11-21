package com.kt.swinno

import java.io.FileInputStream
import java.util.Properties
import org.apache.log4j.Logger

object Common {

  val logger:Logger = Logger.getLogger(this.getClass.getName)
  val prop:Properties = readProperties()

  def getProp:Properties = {
    return prop
  }

  /**
    * Property 파일 read
    * @return
    */
  def readProperties(): Properties = {
    val prop:Properties = new Properties()
    try{
      logger.info("Read Property File")
      val fs:FileInputStream = new FileInputStream("resources/config.properties")
      prop.load(fs)
      fs.close()
      printProps(prop)
      logger.info("설정 파일 읽기 완료")
    }catch{
      case e:Exception => logger.error("설정파일을 읽는 중 오류가 발생했습니다.\n" + e.getMessage)
    }
    prop
  }

  /**
    * Property 파일 key, value 출력
    * @param prop
    */
  def printProps(prop:Properties):Unit = {
    val keySet = prop.keySet()
    val keyIter = keySet.iterator()

    logger.info("Print Properties")
    while(keyIter.hasNext){
      val propKey:String = keyIter.next().toString
      val propVal:String = prop.getProperty(propKey)
      println(f"[$propKey%-25s] - $propVal")
      logger.info("["+propKey+"\t\t] - "+propVal)
    }
  }

}
