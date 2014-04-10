package com.dodopipe.spark.pagerank

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._
import org.apache.spark.bagel



object BagelPageRank extends Logging{

    def apply(sc: SparkContext, inputFile: String,
                                threshold: Double,
                            numPartitions: Int,
                            usePartitioner : Boolean = true) {

        val lines = sc.textFile(inputFile)
                .filter( v => {
                    !v.isEmpty && v(0) != '#'
                })
                .map { line => 
                    val e = line.split("\\s+")
                    if ( e.length < 2 ) {
                        logWarning("Invalid line: " + line) 
                    }
                    (e(0).toString, e(1).toString)
                }
        val vertexIds:RDD[(String,Int)] = (lines.map(v=>(v._1,0)) ++ lines.map(v=>(v._2,0)))
                                                .distinct.cache()
        val vertexIdsAsArray = vertexIds.map(_._1).collect()
        val verticesNum = vertexIds.count()
        val edges = lines.groupBy(_._1).map( v => {
                        val _num = verticesNum
                        val (vid, edgesBuffer) = v
                        val edges = edgesBuffer.map(_._2)
                        (vid,new PRVertex(1.0 / _num, edges.toArray))
                    }).cache()
        var vertices = vertexIds.leftOuterJoin(edges)
                            .map( v =>(v._1,v._2._2))
                            .map{ v =>
                                val _num = verticesNum
                                var _vertexIds = vertexIdsAsArray
                                v._2  match {
                                    case Some(prv) => (v._1, prv) 
                                    case None      => (v._1,new PRVertex(1.0/_num, _vertexIds ))
                                }  
        }
        println("Done parsing input file.")

        if (usePartitioner) {
            vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
        } else {
            vertices = vertices.cache
        }

        val epsilon = 0.01 / verticesNum
        val messages = sc.parallelize(Array[(String, PRMessage)]())
        val utils = new PageRankUtils
        val result = Bagel.run( sc, 
                                vertices, 
                                messages, 
                                combiner = new PRCombiner(),
                                numPartitions = numPartitions)(
                                    utils.computeWithCombiner(verticesNum, epsilon))

        result.map(v => (v._2.value, v._1)).sortByKey( false).foreach(println(_))

        val totalPR = result.map{case (id,vertex) => vertex.value}.reduce(_ + _)
        println("Graph totle pagerank:" + totalPR)

    }
}