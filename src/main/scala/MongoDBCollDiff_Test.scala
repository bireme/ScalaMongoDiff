object MongoDBCollDiff_Test extends App {

  val database_from1: String = "CSVPENTAHO"
  val collection_from1: String = "CSV-DIR-SCALA"
  val collection_from2: String = "who-repo4covid-biblio.etl"
  val collection_out: String = "DIFF_PENTAHO-SPARK"
  val idField: String = "CovNum"
  val database_from2: Option[String] = Option("COVID-19_WHO_Excel")
  val database_out: Option[String] = Option("CSVPENTAHO")
  val host_from1: Option[String] = Option("172.17.1.230")
  val port_from1: Option[Int] = Option(27017)
  val host_from2: Option[String] = Option("172.17.1.71")
  val port_from2: Option[Int] = Option(27017)
  val host_out: Option[String] = Option("172.17.1.230")
  val port_out: Option[Int] = Option(27017)
  val user_from1: Option[String] = None
  val password_from1: Option[String] = None
  val user_from2: Option[String] = None
  val password_from2: Option[String] = None
  val user_out: Option[String] = None
  val password_out: Option[String] = None
  val total: Option[Int] = Option(0)
  val noCompFields: Option[String] = None
  val takeFields: Option[String] = None
  val noUpDate: Boolean = false
  val append: Boolean = false


  val parameters: ParamsMongoDBCollDiff = ParamsMongoDBCollDiff(database_from1, collection_from1, collection_from2, collection_out, idField,
    database_from2, database_out, host_from1, port_from1, host_from2, port_from2, host_out, port_out, user_from1,
    password_from1, user_from2, password_from2, user_out, password_out, total, noCompFields, takeFields, noUpDate, append)

  val variavel = new MongoDBCollDiff()
  variavel.mongoDBCollDiff(parameters)
}