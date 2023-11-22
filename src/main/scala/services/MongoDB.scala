package services

import com.mongodb.client.model.Indexes
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, Observable}

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MongoDB(database: String,
              collection: String,
              host: Option[String] = None,
              port: Option[Int] = None,
              user: Option[String] = None,
              password: Option[String] = None,
              total: Option[Int] = Option(0),
              append: Boolean) {
  require((user.isEmpty && password.isEmpty) || (user.nonEmpty && password.nonEmpty))

  private val hostStr: String = host.getOrElse("localhost")
  private val portStr: String = port.getOrElse(27017).toString
  private val usrPswStr: String = user match {
    case Some(usr)
    => s"$usr:${password.get}@"
    case None => ""
  }

  private val mongoUri: String = s"mongodb://$usrPswStr$hostStr:$portStr"
  private val mongoClient: MongoClient = MongoClient(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(database)
  private val coll: MongoCollection[Document] = {
    if (append)
      dbase.getCollection(collection)
    else dbase.getCollection(collection).drop().results()
    dbase.getCollection(collection)
  }

  def createIndex(indexName: Option[String]): Unit = {
    indexName match {
      case Some(value) =>
        val listIndex: Set[String] = value.split(",").toSet
        listIndex.foreach{index =>
          coll.createIndex(Indexes.descending(index)).results()
        }
      case None => ()
    }
  }

  def findAll: Seq[Document] = new DocumentObservable(coll.find().limit(total.getOrElse(0))).observable.results()

  def insertDocument(doc: String): Unit = coll.insertOne(Document(doc)).results()

  implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: Document => String = doc => doc.toJson()
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: C => String = doc => Option(doc).map(_.toString).getOrElse("")
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: C => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(120, TimeUnit.SECONDS))
  }
}