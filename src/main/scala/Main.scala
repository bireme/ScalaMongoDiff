import operation.CollectionDiffChecker
import models.ParamsMongoDBCollDiff

import scala.util.{Failure, Success}
import java.util.Date

object Main {

  private def usage(): Unit = {
    System.err.println("-database_from1=<name>    - MongoDB database name")
    System.err.println("-collection_from1=<name>  - MongoDB database collection name")
    System.err.println("-collection_from2=<name>  - MongoDB database collection name")
    System.err.println("-collection_out=<name>    - MongoDB database collection name")
    System.err.println("-idField=<name>           - Document identifier field")
    System.err.println("[-database_from2=<name>]  - MongoDB database name")
    System.err.println("[-database_out=<name>]    - MongoDB database name")
    System.err.println("[-host_from1=<name>]      - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_from1=<number>]    - MongoDB server port number. Default value is 27017")
    System.err.println("[-host_from2=<name>]      - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_from2=<number>]    - MongoDB server port number. Default value is 27017")
    System.err.println("[-host_out=<name>]        - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_out=<number>]      - MongoDB server port number. Default value is 27017")
    System.err.println("[-user_from1=<name>])     - MongoDB user name")
    System.err.println("[-password_from1=<pwd>]   - MongoDB user password")
    System.err.println("[-user_from2=<name>])     - MongoDB user name")
    System.err.println("[-password_from2=<pwd>]   - MongoDB user password")
    System.err.println("[-user_out=<name>])       - MongoDB user name")
    System.err.println("[-password_out=<pwd>]     - MongoDB user password")
    System.err.println("[-total=<number>]         - Total documents to be compared")
    System.err.println("[-noCompFields=<name>]    - Fields that should not be compared")
    System.err.println("[-takeFields=<name>]      - Fields that should be taken to the new collection even compared")
    System.err.println("[--noUpDate]              - If present, it will not add the _updd field with the update date")
    System.err.println("[--append]                - If present, will compose the collection without clearing it first")
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 6) usage()

    val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if (split.size == 1) map + ((split(0).substring(2), ""))
        else map + (split(0).substring(1) -> split(1))
    }

    if (!Set("database_from1", "collection_from1", "collection_from2", "collection_out", "idField").forall(parameters.contains)) usage()

    val database_from1: String = parameters("database_from1")
    val collection_from1: String = parameters("collection_from1")
    val collection_from2: String = parameters("collection_from2")
    val collection_out: String = parameters("collection_out")
    val idField: String = parameters("idField")
    val database_from2: Option[String] = parameters.get("database_from2")
    val database_out: Option[String] = parameters.get("database_out")
    val host_from1: Option[String] = parameters.get("host_from1")
    val port_from1: Option[Int] = parameters.get("port_from1").flatMap(_.toIntOption)
    val host_from2: Option[String] = parameters.get("host_from2")
    val port_from2: Option[Int] = parameters.get("port_from2").flatMap(_.toIntOption)
    val host_out: Option[String] = parameters.get("host_out")
    val port_out: Option[Int] = parameters.get("port_out").flatMap(_.toIntOption)
    val user_from1: Option[String] = parameters.get("user_from1")
    val password_from1: Option[String] = parameters.get("password_from1")
    val user_from2: Option[String] = parameters.get("user_from2")
    val password_from2: Option[String] = parameters.get("password_from2")
    val user_out: Option[String] = parameters.get("user_out")
    val password_out: Option[String] = parameters.get("password_out")
    val total: Option[Int] = parameters.get("total").flatMap(_.toIntOption)
    val noCompFields: Option[String] = parameters.get("noCompFields")
    val takeFields: Option[String] = parameters.get("takeFields")
    val noUpDate: Boolean = parameters.contains("noUpDate")
    val append: Boolean = parameters.contains("append")

    val startDate: Date = new Date()
    val params: ParamsMongoDBCollDiff = ParamsMongoDBCollDiff(database_from1, collection_from1, collection_from2, collection_out,
      idField, database_from2, database_out, host_from1, port_from1, host_from2, port_from2, host_out, port_out, user_from1,
      password_from1, user_from2, password_from2, user_out, password_out, total, noCompFields, takeFields, noUpDate, append)

    (new CollectionDiffChecker).collectionDiffChecker(params) match {
      case Success(_) =>
        println(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        println(exception.getMessage)
        System.exit(1)
    }
  }

  private def timeAtProcessing(startDate: Date): String = {
    val endDate: Date = new Date()
    val elapsedTime: Long = (endDate.getTime - startDate.getTime) / 1000
    val minutes: Long = elapsedTime / 60
    val seconds: Long = elapsedTime % 60
    s"Processing time: ${minutes}min e ${seconds}s\n"
  }
}