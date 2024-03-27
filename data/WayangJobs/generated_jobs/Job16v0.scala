
 /*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.wayang.training
 import org.apache.wayang.api._
 import org.apache.wayang.core.api.{Configuration, WayangContext}
 import org.apache.wayang.core.plugin.Plugin
 import org.apache.wayang.apps.tpch.data.{Customer, Order, LineItem, Nation, Part, Partsupp, Supplier} 
 import org.apache.wayang.core.plugin.Plugin
 import java.sql.Date
 import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}
 import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Job16v0 {

def buildPlan(args: Array[String]): DataQuanta[Any] {
 println("running Job: Job16v0")

 val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
 val plugins = Parameters.loadPlugins(args(0))
 val datapath ="file:///var/www/html/data/" 

 implicit val configuration = new Configuration
 val wayangCtx = new WayangContext(configuration)
 plugins.foreach(wayangCtx.register)
 val planBuilder = new PlanBuilder(wayangCtx)
 .withJobName(s"TPC-H (${this.getClass.getSimpleName})")

   
 val source0 = planBuilder
 .readTextFile(datapath + "lineItem")
 .withName("Read lineItem")
 .map(LineItem.parseCsv)
 .map(LineItem.toTuple)
 .withName("Parse lineItem to tuple")
    

 val source3 = planBuilder
 .readTextFile(datapath + "partsupp")
 .withName("Read partsupp")
 .map(Partsupp.parseCsv)
 .map(Partsupp.toTuple)
 .withName("Parse partsupp to tuple")
    

val map2 = source3.map(x=>x)
                

val join1 = source0.keyBy[Long](_._3).join(map2.keyBy[Long](_._2)).map(x => x.field1)
       

 val source7 = planBuilder
 .readTextFile(datapath + "supplier")
 .withName("Read supplier")
 .map(Supplier.parseCsv)
 .map(Supplier.toTuple)
 .withName("Parse supplier to tuple")
    

val map6 = source7.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                

val filter5 = map6.filter(x=>x._4 == 12.0)
      

val join4 = join1.keyBy[Long](_._2).join(filter5.keyBy[Long](_._1)).map(x => x.field1)
       

val group8 = join4.reduceByKey(_._3, (t1,t2) => t1)
          

val map9 = group8.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
       

val map10 = map9.map(x=>x)
                

val group11 = map10.reduceByKey(_._3, (t1,t2) => t1)
          

 return group11
      

 }
}  
  