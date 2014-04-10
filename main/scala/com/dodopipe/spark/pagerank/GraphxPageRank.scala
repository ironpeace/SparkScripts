package com.dodopipe.spark.pagerank

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._

object GraphxPageRank extends Logging {

    def withIterationSteps[VD: ClassTag, ED: ClassTag](
                    graph: Graph[VD,ED], 
                  stepNum: Int, 
           dumplingFactor: Double = 0.85) : Graph[Double, Double] = {

        val verticesNum = graph.vertices.count();
        val teleportationPosibility = (1.0 - dumplingFactor)  / verticesNum; 
        val initialVertexValue = 0.0 ;
        val initialMessage = 0.0 

        val pagerankGraph: Graph[(Double), Double] = 
                preparePagerankGraph(graph, initialVertexValue)


        def vertexProgram(id: VertexId, 
                       value: Double, 
                      msgSum: Double): Double = {
            val _tele = teleportationPosibility
            val _factor = dumplingFactor
            _tele +  _factor * msgSum
        }

        def sendMessage(edge: EdgeTriplet[Double, Double]) = {
                Iterator((edge.dstId, edge.srcAttr * edge.attr))
        }

        def messageCombiner(a: Double, b: Double): Double = a + b 

        Pregel(pagerankGraph, 
               initialMessage, 
               stepNum, 
               activeDirection = EdgeDirection.Out)(vertexProgram,
                                                    sendMessage,
                                                    messageCombiner)

    }

    def withThreshold[VD: ClassTag, ED: ClassTag](
                   graph: Graph[VD, ED], 
                   delta: Double, 
          dumplingFactor: Double = 0.85): Graph[Double, Double] = {

        val verticesNum = graph.vertices.count();
        val teleportationPosibility = 1 / verticesNum;
        val initialVertexValue = ( 0.0, 0.0 )
        val initialMessage = (1.0 - dumplingFactor) / dumplingFactor

        val pagerankGraph: Graph[(Double,Double), Double] =
                preparePagerankGraph(graph, initialVertexValue) 

        def vertexProgram(id: VertexId, 
                       value:(Double, Double), 
                      msgSum: Double): (Double, Double) = {
            val (oldPR, lastDelta) = value
            //val newPR = teleportationPosibility + (1.0 - dumplingFactor) * msgSum
            val newPR = oldPR + dumplingFactor * msgSum
            (newPR, newPR - oldPR)
        }

        def sendMessage(edge: EdgeTriplet[(Double,Double),Double]) = {

            if (edge.srcAttr._2 > delta ) {
                Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
            }else {
                Iterator.empty
            }
        }

        def messageCombiner(a: Double, b: Double): Double = a + b

        Pregel(pagerankGraph, 
               initialMessage,
               activeDirection = EdgeDirection.Out )( vertexProgram,
                                                     sendMessage,
                                                     messageCombiner)
               .mapVertices((vid,attr) => attr._1)
    } 

    protected def preparePagerankGraph[VD: ClassTag, ED: ClassTag, IVD: ClassTag](
                        graph: Graph[VD, ED],
                    initValue: IVD): Graph[IVD, Double] = {

        // compute the number of outer links for every vertex.
        graph.outerJoinVertices(graph.outDegrees) { 
            (vid, value, deg) => deg.getOrElse(0)
        }
        // set 1.0 / num(outerlinks) as edge's weight
        .mapTriplets(e => 1.0 / e.srcAttr)
        // initialize the vertex's value
        .mapVertices((id,attr) => initValue)
        .cache()
    } 

}