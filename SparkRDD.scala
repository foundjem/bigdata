
def PeopleYouMayKnowRDD(){ 
		
		import org.apache.spark.SparkContext
		import org.apache.spark.SparkContext._
		import org.apache.spark.SparkConf
		import scala.collection.mutable
		import java.util.HashMap
		import scala.collection

    val now = System.nanoTime
		//  create our simple usership list
		def parsereadLine(readLine: String): (Int, Array[String]) = {
		  (Integer.parseInt(readLine.substring(0, readLine.indexOf("\t"))), readLine.substring(readLine.indexOf("\t")+1).split(","))
		}

		def convertToArray(strings: Array[String]): Array[Int] = { 
		  strings.filter({ x => !x.isEmpty() }).map({ x => Integer.parseInt(x) }) 
		}

		// Existing relationships
		// linking file using spark content similar to parsing: (userID, IDs of users)
		//From the data.dat file,  create our simple user-friendship list, APPLYING DIVIDE AND CONQUER METHOD IS VERY PRACTICAL HERE
		// We split the dataset into four which helps to reduce the processing time
		val inRelationshipAlready = sc.textFile("data.dat", 4).map { parsereadLine }.mapValues( convertToArray );

		//Suggestion relationships
		// If user 1 is friend with say, user k and m, implies our suggestion of k to m becoming friend and vice versa
		def pairwiswComm(proposes: Array[Int]): TraversableOnce[(Int, Int)] = { proposes.combinations(2).map { propose => (propose(0), propose(1)) }
		             .flatMap { propose => Iterator(propose, (propose._2, propose._1)) }
		}
		// We then go ahead to create propose from the users Array
		val proposesRDD = inRelationshipAlready.map( x => x._2 ).flatMap( pairwiswComm ) 

		//we append set1 to set2 --->set2 will be modified!
		/*def appendSet(propose:mutable.Set[Int], newPropose:mutable.Set[Int])={
		  propose.foreach { element=>  newPropose.add(element) }
		}
		*/
		//regrouping the RDD of user's proposes :
		def mergepropose(proposes: mutable.HashMap[Int, Int], newpropose: Int): mutable.HashMap[Int, Int] = {
		  proposes.get(newpropose) match {
		    case None => proposes.put(newpropose, 1)
		    case Some(x) => proposes.put(newpropose, x + 1)
		  }
		  proposes
		}
		def mergeproposes(proposes: mutable.HashMap[Int, Int], unionized: mutable.HashMap[Int, Int]) = {
		  val keys = proposes.keys ++: unionized.keys
		  keys.foreach { key =>
		    proposes.get(key) match {
		      case None => proposes.put(key, unionized.getOrElse(key, 0))
		      case Some(x) => proposes.put(key, x + unionized.getOrElse(key, 0))
		    }
		  }
		  proposes
		}
		//I added this constriant to optimize my propose strategy
		def filterRareproposes(proposes: mutable.HashMap[Int, Int]): scala.collection.Set[Int] = {
		  proposes.filter(p => p._2 >= 3).keys
		}
		// Reducing suggested RDD count by user
		val proposesByPersonRDD = proposesRDD.combineByKey((person: Int) => new mutable.HashMap[Int, Int](),mergepropose, mergeproposes).mapValues( filterRareproposes )
		//Now, we can filter the propose in considerations to the exisiting friendships
		val proposesCleanedRDD = proposesByPersonRDD.join(inRelationshipAlready).mapValues (_ match { case (proposes, alreadyKnownFriendsByPerson) => {
		    proposes -- alreadyKnownFriendsByPerson
		  }})
	//Measuring execution time
    val micros = (System.nanoTime - now) 
    val duration = Duration(micros, MILLISECONDS)
    println("%d microseconds".format(duration.toSeconds))

 }
