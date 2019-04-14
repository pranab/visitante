/*
 * visitante-spark: log and search analysis on spark
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

package org.visitante.spark.search

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.spark.common.Record

object NormDiscCumGain extends JobConfiguration {
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "normDiscCumGain"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delimIn", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delimOut", ",")
	   val scoreFilePath = getMandatoryStringParam(appConfig, "score.filePath", "missing score file path")
	   val relQueryIdOrd = getMandatoryIntParam(appConfig, "rel.queryIdOrd", "missing relevance data query Id ordinal")
	   val relDocIdOrd = getMandatoryIntParam(appConfig, "rel.docIdOrd", "missing relevance data doc Id ordinal")
	   val relValueOrd = getMandatoryIntParam(appConfig, "rel.valueOrd", "missing relevance value ordinal")
	   val scoreQueryIdOrd = getMandatoryIntParam(appConfig, "score.queryIdOrd", "missing score data query Id ordinal")
	   val scoreDocIdOrd = getMandatoryIntParam(appConfig, "score.docIdOrd", "missing score data doc Id ordinal")
	   val scoreValueOrd = getMandatoryIntParam(appConfig, "score.valueOrd", "missing score value ordinal")
	   val outputAggrRel = getBooleanParamOrElse(appConfig, "output.aggrRel", false)
	   val relAggrOutPath = getOptionalStringParam(appConfig, "rel.aggrOutPath")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   //relevance data
   	   val aggrRel = data.map(line => {
   		   val items = BasicUtils.getTrimmedFields(BasicUtils.splitOnFirstOccurence(line, fieldDelimIn, true))
   		   val keyRec = Record(2)
   		   keyRec.addString(items(relQueryIdOrd))
   		   keyRec.addString(items(relDocIdOrd))
   		   val relScore = items(relValueOrd).toDouble
   		   (keyRec, relScore)
   	   }).reduceByKey((v1, v2) => v1 + v2)

   	   //insert value type, relevance or score
	   val keyedRel = aggrRel.map(r => {
   		   val valRec = Record(2)
   		   valRec.addInt(2)
	       valRec.addDouble(r._2)
	       (r._1, valRec)
	   }).cache

	   if (outputAggrRel) {
   	     relAggrOutPath match {
   	       case Some(outPath) => {
   	         keyedRel.map(r => r._1.toString() + fieldDelimOut + BasicUtils.formatDouble(r._2.getDouble(1), precision)).
   	         	saveAsTextFile(outputPath)
   	       }
   	       case None => BasicUtils.assertFail("missing aggregate relevance output file path")
   	     }
   	   }
	   
	   //score data
	   val scoreData = sparkCntxt.textFile(scoreFilePath)
   	   val keyedScore = scoreData.map(line => {
   		   val items = BasicUtils.getTrimmedFields(BasicUtils.splitOnFirstOccurence(line, fieldDelimIn, true))
   		   val keyRec = Record(2)
   		   keyRec.addString(items(scoreQueryIdOrd))
   		   keyRec.addString(items(scoreDocIdOrd))
   		   val valRec = Record(2)
   		   valRec.addInt(1)
   		   valRec.addDouble(items(scoreValueOrd).toDouble)
   		   (keyRec, valRec)
   	   })	
   	   
   	   //union of score and relevance, key by query Id
   	   val unRecs = (keyedScore ++ keyedRel).groupByKey.map(r => {
   	     val vArr = r._2.toArray
   	     if (vArr.size != 2) {
   	       BasicUtils.assertFail("expecting 2 value records found " + vArr.size)
   	     }
   	     val valueRec = Record(3)
   	     valueRec.addString(0, r._1.getString(1))
   	     for (i <- 0 to 1) {
	   	     if (vArr(i).getInt(0) == 1) {
	   	       //score
	   	       valueRec.addDouble(1, vArr(i).getDouble(1))
	   	     } else {
	   	       //relevance
	   	       valueRec.addDouble(2, vArr(i).getDouble(1))
	   	     }
   	     }
   	     (r._1.getString(0), valueRec)
   	   })
   	   
   	   //calculate NCDG
   	   val ncdgrecs = unRecs.groupByKey.map(r => {
   	     val values = r._2.toArray
   	     val valSortedByScore = values.sortWith((r1, r2) => r1.getDouble(1) > r2.getDouble(1)).zipWithIndex
   	     val cdg = calculateCumDiscGain(valSortedByScore)
   	     val valSortedByrel = values.sortWith((r1, r2) => r1.getDouble(2) > r2.getDouble(2)).zipWithIndex
   	     val cdgMax = calculateCumDiscGain(valSortedByrel)
   	     val cdgNorm = cdg / cdgMax
   	     r._1 + fieldDelimOut +  BasicUtils.formatDouble(cdgNorm, precision)
   	   })
   	   
       if (debugOn) {
         val records = ncdgrecs.collect.sliding(10)
         records.foreach(r => println(r))
       }
	   
	   if(saveOutput) {	   
	     ncdgrecs.saveAsTextFile(outputPath) 
	   }
   	   
   }
   
    /**
   	 * @param recs
   	 * @return
   	 */
   	def calculateCumDiscGain(recs: Array[(org.chombo.spark.common.Record, Int)]) : Double = {
      var cdg = 0.0
      recs.foreach(r => {
        val rank = r._2 + 1
        cdg += (Math.pow(2, r._1.getDouble(2)) -1) / Math.log(rank + 1)
      })
      cdg
	}
  
}