import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.mongodb.scala.Document
import org.mongodb.scala.bson.BsonArray

import java.util.Date
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class MongoDBCollDiff {

  private def mongoDBCollDiff(database_from1: String,
                           collection_from1: String,
                           collection_from2: String,
                           collection_out: String,
                           idField: String,
                           database_from2: Option[String],
                           database_out: Option[String],
                           host_from1: Option[String],
                           port_from1: Option[Int],
                           host_from2: Option[String],
                           port_from2: Option[Int],
                           host_out: Option[String],
                           port_out: Option[Int],
                           user_from1: Option[String],
                           password_from1: Option[String],
                           user_from2: Option[String],
                           password_from2: Option[String],
                           user_out: Option[String],
                           password_out: Option[String],
                           total: Option[Int],
                           noCompFields: Option[String],
                           takeFields: Option[String],
                           noUpDate: Boolean,
                           append: Boolean): Try[Unit] = {
    Try {
      val mongo_instance1: MongoDB = new MongoDB(database_from1, collection_from1, host_from1, port_from1, user_from1, password_from1, total, true)
      val mongo_instance2: MongoDB = new MongoDB(database_from2.getOrElse(""), collection_from2, host_from2, port_from2, user_from2, password_from2, total, true)
      val mongo_instanceOut: MongoDB = new MongoDB(database_out.getOrElse(""), collection_out, host_out, port_out, user_out, password_out, total, append)

      val docs_instance1: Seq[Document] = mongo_instance1.findAll
      val docs_instance2: Seq[Document] = mongo_instance2.findAll

      val documentsCompared: Seq[Array[(String, AnyRef)]] = compareDocuments(docs_instance1, docs_instance2, idField, noCompFields, takeFields)
      val documentsFinal = updateField_updd(documentsCompared, noUpDate)
      val listJson: Seq[String] = documentsFinal.map(f => Json(DefaultFormats).write(f.toMap.map(f => (f._1, f._2))))
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
    } yield (identifierField, doc1.get(identifierField).get.asString().getValue) +: (keyUpdd, doc1.get(keyUpdd).get.asString().getValue) +: result.toArray

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