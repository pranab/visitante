/*
 * chombo-spark: etl on spark
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.visitante.spark.basic

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.visitante.util.LogParser


/**
 * Extracts session data from web server log
 * @author pranab
 *
 */
object SessionExtractor extends JobConfiguration {
   /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "sessionExtractor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val logFormatStd = getStringParamOrElse(appConfig, "log.formatStd", "NCSA")
	   val logFieldList = getMandatoryStringListParam(appConfig, "log.fieldList", "missing output field list")
	   val sessionIdName = getMandatoryStringParam(appConfig, "session.idName")
	   val userIdName = getOptionalStringParam(appConfig, "user.idName");
	   val dateTimeFormatStr = getStringParamOrElse(appConfig, "date.format", BasicUtils.EPOCH_TIME)
	     
	     
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val parsedLines = data.mapPartitions(part => {
	     val parser = userIdName match {
	       case (Some(id:String)) => new LogParser(logFormatStd, sessionIdName, id, dateTimeFormatStr)
	       case None => new LogParser(logFormatStd, sessionIdName,  dateTimeFormatStr)
	     }
	     
	     val lines = part.map(line => {
	       parser.parse(line)
	       if (parser.contains(LogParser.SESSION_ID)) {
	    	   val values = parser.getStringValues(logFieldList)
	    	   values.mkString(fieldDelimOut)
	       } else {
	         "xx"
	       }
	     })
	     lines
	   }, true)
           
	   val filtLines = parsedLines.filter(line => !line.equals("xx"))
	   
       if (debugOn) {
         filtLines.foreach(line => println(line))
       }
	   
	   if(saveOutput) {	   
	     filtLines.saveAsTextFile(outputPath)
	   }
   }
}