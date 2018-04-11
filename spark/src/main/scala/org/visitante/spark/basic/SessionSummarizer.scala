/*
 * visitante-spark: log analysis on spark
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
import org.visitante.util.LogParser
import org.visitante.util.StandardLogParser
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record

object SessionSummarizer extends JobConfiguration {
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "sessionSummarizer"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	   val fieldDelimOut = appConfig.getString("field.delim.out")
	   val logFieldList = getMandatoryStringListParam(appConfig, "log.fieldList", "missing field list").asScala.toArray
	   val recSize = logFieldList.length
	   val sessIdOrd = logFieldList.indexOf(StandardLogParser.SESSION_ID)
	   val dateTimeOrd = logFieldList.indexOf(StandardLogParser.DATE_TIME_EPOCH)
	   val timeSpentOrd = logFieldList.indexOf(StandardLogParser.TIME_ON_PAGE)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   val parser = new StandardLogParser()
	   
	   val data = sparkCntxt.textFile(inputPath)
	   val keyedRecs = data.map(line => {
	     val items = line.split(fieldDelimIn)
	     val rec = Record(recSize + 1)
	     for (i <- 0 to recSize-1) {
	       rec.addStringAsTyped(items(i), parser.getFieldType(logFieldList(i)))
	     }
	     rec.addInt(1)
	     rec
	   }).keyBy(v => v.getString(sessIdOrd))
	   
	   //aggregate
	   val aggrRecs = keyedRecs.reduceByKey((r1,r2) => {
	       val timeSpent1 = r1.getLong(recSize-1)
	       val count1 = r1.getInt(recSize)
	       val timeSpent2 = r2.getLong(recSize-1)
	       val count2 = r2.getInt(recSize)
	     
	     val rec = if (r1.getLong(dateTimeOrd) < r2.getLong(dateTimeOrd)) {
	    	 val rec = Record(r1)
	         r1.addLong(recSize-1, timeSpent1 + timeSpent2)
	         r1.addInt(recSize, count1 + count2)
	         rec
	     } else {
	    	 val rec = Record(r2)
	         r2.addLong(recSize-1, timeSpent1 + timeSpent2)
	         r2.addInt(recSize, count1 + count2)
	         rec
	     }
	     rec
	   })
	   
	   //summary
	   val sessionSummaryRecs = aggrRecs.map(kv => {
	     val r = kv._2
	     val rec = Record(r.size + 1, r)
	     val timeSpent = r.getLong(r.size - 2)
	     val count = r.getInt(r.size - 1)
	     val avTimeSpent = (timeSpent/(count -1)).toInt
	     rec.addInt(avTimeSpent)
	     rec
	   })
	   
       if (debugOn) {
         sessionSummaryRecs.foreach(line => println(line))
       }
	   
	   if(saveOutput) {	   
	     sessionSummaryRecs.saveAsTextFile(outputPath)
	   }
	   
   }
}