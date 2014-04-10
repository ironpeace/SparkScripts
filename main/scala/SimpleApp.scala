/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object SimpleApp { 
  def main(args: Array[String]) { 
  	if(args(0) == "1") sample1(args)
  	if(args(0) == "2") sample2(args)
  	if(args(0) == "3") sample3(args)
  	if(args(0) == "4") sample4(args)
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def sample1(args: Array[String]){
    val logFile = "README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "YOUR_SPARK_HOME",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def sample2(args: Array[String]){
	// Connect to the Spark cluster
	val sc = new SparkContext("local", "Simple App")

	// Load my user data and parse into tuples of user id and attribute list
	val users = (sc.textFile("graphx/data/users.txt")
	  .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

	// Parse the edge data which is already in userId -> userId format
	val followerGraph = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")

	// Attach the user attributes
	val graph = followerGraph.outerJoinVertices(users) {
	  case (uid, deg, Some(attrList)) => attrList
	  // Some users may not have attributes so we set them as empty
	  case (uid, deg, None) => Array.empty[String]
	}

	// Restrict the graph to users with usernames and names
	val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

	// Compute the PageRank
	val pagerankGraph = subgraph.pageRank(0.001)

	// Get the attributes of the top pagerank users
	val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
	  case (uid, attrList, Some(pr)) => (pr, attrList.toList)
	  case (uid, attrList, None) => (0.0, attrList.toList)
	}

	println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }

  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	 def sample3(args: Array[String]){
		val sc = new SparkContext("local", "TransferGraph")

		val ctf = sc.textFile("mydata/customers.csv")
		var ttf = sc.textFile("mydata/transfer_history.csv")

		val customers:RDD[(VertexId, (String, String, String, String))] 
			= ctf.map(line => line.split(",")).map(parts => 
				(parts(0).toLong, (parts(1), parts(2), parts(3), parts(4)))
			)

		val transfers: RDD[Edge[(String, Long, String)]]
			= ttf.map(line => line.split(",")).map(parts =>
				Edge(parts(1).toLong, parts(2).toLong, (parts(0), parts(3).toLong, parts(4)))
			)

		val graph = Graph(customers, transfers)

		println("numEdges : " + graph.numEdges)

		val bank101_count = graph.vertices.filter {
			case (id, (bank_code, branch_no, cif_no, name)) => bank_code == "101"
		}.count

		println("bank101_count : " + bank101_count)

		val srcLargerThanDst_count = graph.edges.filter {
			case Edge(src, dst, prop) => src > dst
		}.count

		println("srcLargerThanDst_count : " + srcLargerThanDst_count)	

		val over200amount_count = graph.edges.filter {
			case Edge(src, dst, prop) => prop._2 >= 200
		}.count

		println("over200amount_count : " + over200amount_count)

		val facts: RDD[String] =
			graph.triplets.map(triplet =>
				triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

		facts.collect.foreach(println(_))

		println("All vertices ~~~~~~~~~~~~~~~")
		graph.vertices.collect.foreach(println(_))
		println("inDegrees ~~~~~~~~~~~~~~~")
		graph.inDegrees.collect.foreach(println(_))
		println("outDegrees ~~~~~~~~~~~~~~~")
		graph.outDegrees.collect.foreach(println(_))

		println("initial graph ~~~~~~~~~~~~~~~")
		// graph.triplets.collect.foreach(t => 
		// 	println("%s(%s-%s-%s) send %s(%s-%s-%s) ¥%d at %s".format(
		// 		t.srcAttr._4, t.srcAttr._1, t.srcAttr._2, t.srcAttr._3,
		// 		t.dstAttr._4, t.dstAttr._1, t.dstAttr._2, t.dstAttr._3,
		// 		t.attr._1, t.attr._2)
		// 	)
		// )
		// graph.triplets.collect.foreach(t => printGraph(
		// 	t.srcAttr._4, t.srcAttr._1, t.srcAttr._2, t.srcAttr._3,
		// 	t.dstAttr._4, t.dstAttr._1, t.dstAttr._2, t.dstAttr._3,
		// 	t.attr._2, t.attr._1)
		// )

		// これ↓だと、何故か同じ文言が３行書きだされるだけになる。
		// graph.triplets.collect.foreach(t => 
		// 	println("[srcId:" + t.srcId + ", srcAttr:" + t.srcAttr + "] -> " 
		// 		+ t.attr + " ->[dstId:" + t.dstId + ", dstAttr:" 
		// 		+ t.dstAttr + "]")
		// )

		// こうやって、一度変数に入れるとちゃんと３つ分出力される
		val facts_init: RDD[String] =
			graph.triplets.map(t =>
				"[srcId:" + t.srcId + ", srcAttr:" + t.srcAttr + "] -> " 
				+ t.attr + " ->[dstId:" + t.dstId + ", dstAttr:" 
				+ t.dstAttr + "]")
		facts_init.collect.foreach(println(_))

		println("mapVertices ~~~~~~~~~~~~~~~")
		val graph2 = graph.mapVertices({(id, attr) =>
			(id * 100, attr)
		})

		// graph.mapVertices({(id, attr) =>
		// 	(id * 100, attr)
		// }).triplets.collect.foreach(t => 
		// println("[srcId:" + t.srcId + ", srcAttr:" + t.srcAttr + "] -> " 
		// 	+ t.attr + " ->[dstId:" + t.dstId + ", dstAttr:" 
		// 	+ t.dstAttr + "]")
		// )

		graph2.vertices.collect.foreach(println(_))
		graph2.edges.collect.foreach(println(_))

		// println("graph2.numVertices : " + graph2.numVertices)
		// println("graph2.numEdges : " + graph2.numEdges)

		// graph2.triplets.collect.foreach(t => 
		// 	println(t.srcAttr._1 + " is the " + t.attr + " of " + t.dstAttr._1)
		// )

	}

	def printGraph(
		src_name:String, src_bank_code:String, src_branch_no:String, src_account_no:String,
		dst_name:String, dst_bank_code:String, dst_branch_no:String, dst_account_no:String,
		amount:Long, date:String){
		println("%s(%s-%s-%s) send %s(%s-%s-%s) ¥%d at %s".format(
			src_name, src_bank_code, src_branch_no, src_account_no,
			dst_name, dst_bank_code, dst_branch_no, dst_account_no,
			amount, date)
		)
	}

	/**
	def printGraph(g:Graph[VD, ED]){
		g.triplets.collect.foreach(t => 
			println("%s(%s-%s-%s) send %s(%s-%s-%s) ¥%d at %s".format(
				t.srcAttr._4, t.srcAttr._1, t.srcAttr._2, t.srcAttr._3,
				t.dstAttr._4, t.dstAttr._1, t.dstAttr._2, t.dstAttr._3,
				t.attr._1, t.attr._2)
			)
		)
	}
	**/


	//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def sample4(args: Array[String]){
		val sc = new SparkContext("local", "DocWorkCount")
		val tf = sc.textFile("mydata/graphx_doc.txt")

		val counts = tf.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)

		counts.saveAsTextFile("mydata/graphx_doc_counted")

		println("total wordcount : " + tf.flatMap(line => line.split(" ")).count)


	}


}
