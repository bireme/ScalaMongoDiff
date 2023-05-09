import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.mongodb.scala.Document
import org.mongodb.scala.bson.BsonArray

import java.util.Date
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Failure, Success, Try}

class MongoDBCollDiff {

  def mongoDBCollDiff(params: ParamsMongoDBCollDiff): Try[Unit] = {
    Try {
      val mongo_instance1: MongoDB = new MongoDB(params.database_from1, params.collection_from1, params.host_from1, params.port_from1, params.user_from1, params.password_from1, params.total, true)
      val mongo_instance2: MongoDB = new MongoDB(params.database_from2.getOrElse(params.database_from1), params.collection_from2, params.host_from2, params.port_from2, params.user_from2, params.password_from2, params.total, true)
      val mongo_instanceOut: MongoDB = new MongoDB(params.database_out.getOrElse(params.database_from1), params.collection_out, params.host_out, params.port_out, params.user_out, params.password_out, params.total, params.append)

      val docs_instance1: Seq[Document] = mongo_instance1.findAll
      val docs_instance2: Seq[Document] = mongo_instance2.findAll
      println(s"${docs_instance1.length} - docs_instance1")
      println(s"${docs_instance2.length} - docs_instance2")

      val documentsCompared: Seq[Array[(String, AnyRef)]] = compareDocuments(docs_instance1, docs_instance2, params.idField, params.noCompFields, params.takeFields)
      println(s"${documentsCompared.length} - documentsCompared")
      println(s"${params.idField} - idField")
      println(s"${params.noCompFields} - noCompFields")
      println(s"${params.takeFields} - takeFields")

      val documentsFinal = updateField_updd(documentsCompared, params.noUpDate)
      println(s"${documentsFinal.length} - documentsFinal")

      val listJson: Seq[String] = documentsFinal.map(f => Json(DefaultFormats).write(f.toMap.map(f => (f._1, f._2))))
      println(s"${listJson.length} - listJson")
      listJson.sorted.foreach(mongo_instanceOut.insertDocument)
    }
  }

  private def compareDocuments(docs_list1: Seq[Document], docs_list2: Seq[Document], identifierField: String, noCompFields: Option[String], takeFields: Option[String]): Seq[Array[(String, AnyRef)]] = {

    val noCompFieldsParam: Array[String] = noCompFields.getOrElse("_id").split(",") :+ "_id"
    val takeFieldsParam: Array[String] = takeFields.getOrElse(identifierField).split(",") :+ identifierField

    val docsListOneDiffDocsListTwo: Seq[Document] = compareDocumentsBetweenLists(docs_list1, docs_list2, takeFieldsParam)
    val docsListTwoDiffDocsListOne: Seq[Document] = compareDocumentsBetweenLists(docs_list2, docs_list1, takeFieldsParam)

    val docsListOneValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo, identifierField)
    val listDocsOneWithoutMongoId: Seq[Document] = docsListOneValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    val docsListTwoValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListTwoDiffDocsListOne, identifierField)
    val listDocsTwoWithoutMongoId: Seq[Document] = docsListTwoValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    compareDocuments(listDocsOneWithoutMongoId, listDocsTwoWithoutMongoId, takeFieldsParam, identifierField)
  }

  private def compareDocuments(seq1: Seq[Document], seq2: Seq[Document], takeFieldsParam: Array[String], identifierField: String): Seq[Array[(String, AnyRef)]] = {

    val keyUpdd: String = "_updd"

    val resultSeq = for {
      doc1 <- seq1
      doc2 <- seq2
      if doc1.get(identifierField) == doc2.get(identifierField)
      keys = doc1.keySet.intersect(doc2.keySet) ++ doc1.keySet.diff(doc2.keySet) ++ doc2.keySet.diff(doc1.keySet)
      result = keys.flatMap(key => {
        if (key != identifierField && key != keyUpdd) {
          val value1 = checkIsArrayOrString(doc1, doc2, key)
          val value2 = checkIsArrayOrString(doc2, doc1, key)
          if (value1._2 != value2._2 || takeFieldsParam.contains(value1._1)) Some((key, Array(value1._2, value2._2))) else None
        } else None
      })
      if result.nonEmpty
    } yield (identifierField, doc1.get(identifierField).get.asString().getValue) +: result.toArray

    resultSeq.filter(_.nonEmpty).map(f => f.map(h => (h._1, h._2)))
  }

  private def checkIsArrayOrString(doc: Document, docCompare: Document, key: String): (String, AnyRef) = {

    val docAllFields: Document = if (doc.contains(key)) doc else doc.updated(key, "")
    val docIsArray: Boolean = isValueArray(key, docAllFields)

    if (docIsArray) {
      val listFields: Array[String] = docAllFields.get(key).get.asArray().getValues.asScala.map(_.asString().getValue).toArray
      val docAllFieldsArray: Document = if (docCompare.contains(key)) docCompare else docCompare.updated(key, BsonArray(""))
      val isDocValueArray: Boolean = isValueArray(key, docAllFieldsArray)

      if (isDocValueArray) {
        val listFieldsArray: Array[String] = docAllFieldsArray.get(key).get.asArray().getValues.asScala.map(_.asString().getValue).toArray
        val listFieldsFull = listFields.diff(listFieldsArray)
        (key, listFieldsFull)
      } else (key, docAllFields.get(key).get.asString().getValue)
    }else (key, docAllFields.get(key).get.asString().getValue)
  }

  private def isValueArray(key: String, docAllFields: Document): Boolean = {
    docAllFields.get(key).get.getBsonType.name() == "ARRAY"
  }

  private def deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo: Seq[Document], identifierField: String): Seq[Document] = {
    docsListOneDiffDocsListTwo.filterNot(f => f.isEmpty || f.contains("_id") && f.size == 1 || f.contains("_id") && f.contains(identifierField) && f.size <= 2)
  }

  private def compareDocumentsBetweenLists(list1: Seq[Document], list2: Seq[Document], takeFieldsParam: Array[String]): Seq[Document] = {
    val listDocumentsCompared: Seq[Document] = list1.map(doc1 => doc1.filter {
      field1 =>
        if (!takeFieldsParam.contains(field1._1))
          !list2.exists(doc2 => doc2.exists(field2 => field1.equals(field2)))
        else true
    })
    listDocumentsCompared
  }

  private def updateField_updd(datalListFinal: Seq[Array[(String, AnyRef)]], noUpDate: Boolean): Array[Array[(String, AnyRef)]] = {

    val dataListFinalRefactored = if (noUpDate) {
      datalListFinal
    } else {
      val dataWithNewUpdd = datalListFinal.map(f => f.filterNot(h => h._1 == "_updd"))
      dataWithNewUpdd.map(f => f.appended("_updd", new Date().toString))
    }
    dataListFinalRefactored.toArray
  }
}

object MongoDBCollDiff {

  private def usage(): Unit = {
    System.err.println("Error params!")
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

    val params: ParamsMongoDBCollDiff = ParamsMongoDBCollDiff(database_from1, collection_from1, collection_from2, collection_out,
      idField, database_from2, database_out, host_from1, port_from1, host_from2, port_from2, host_out, port_out, user_from1,
      password_from1, user_from2, password_from2, user_out, password_out, total, noCompFields, takeFields, noUpDate, append)

    (new MongoDBCollDiff).mongoDBCollDiff(params) match {
      case Success(_) => System.exit(0)
      case Failure(exception) =>
        println(exception.getMessage)
        System.exit(1)
    }
  }
}