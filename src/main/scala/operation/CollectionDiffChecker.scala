package operation

import models.ParamsMongoDBCollDiff
import services.MongoDB
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.mongodb.scala.Document
import org.mongodb.scala.bson.collection.immutable.Document.fromSpecific
import org.mongodb.scala.bson.{BsonArray, BsonValue}

import java.util.Date
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class CollectionDiffChecker {

  def collectionDiffChecker(params: ParamsMongoDBCollDiff): Try[Unit] = {
    Try {
      System.out.println("\n")
      println(s"Collection comparison started - ScalaMongoDiff ${new Date()}")

      val mongo_instance1: MongoDB = new MongoDB(params.database_from1, params.collection_from1, params.host_from1, params.port_from1, params.user_from1, params.password_from1, params.total, true)
      val mongo_instance2: MongoDB = new MongoDB(params.database_from2.getOrElse(params.database_from1), params.collection_from2, params.host_from2, params.port_from2, params.user_from2, params.password_from2, params.total, true)
      val mongo_instanceOut: MongoDB = new MongoDB(params.database_out.getOrElse(params.database_from1), params.collection_out, params.host_out, params.port_out, params.user_out, params.password_out, params.total, params.append)

      val docs_instance1: Seq[Document] = mongo_instance1.findAll
      val docs_instance2: Seq[Document] = mongo_instance2.findAll
      println(s" * Collection ${params.collection_from1} Total: ${docs_instance1.length}")
      println(s" * Collection ${params.collection_from2} Total: ${docs_instance2.length}")

      val documentsCompared: Seq[Array[(String, AnyRef)]] = getDiffDocuments(docs_instance1, docs_instance2, params.idField, params.noCompFields, params.takeFields)
      val documentsFinal = updateField_updd(documentsCompared, params.noUpDate)

      val listJson: Seq[String] = documentsFinal.map(f => Json(DefaultFormats).write(f.toMap.map(f => (f._1, f._2))))
      println(s" * ${listJson.length} Writings")
      listJson.sorted.foreach(mongo_instanceOut.insertDocument)

      mongo_instanceOut.createIndex(params.indexName)
    }
  }

  private def getDiffDocuments(docs_list1: Seq[Document], docs_list2: Seq[Document], identifierField: String, noCompFields: Option[String], takeFields: Option[String]): Seq[Array[(String, AnyRef)]] = {

    val noCompFieldsParam: Array[String] = noCompFields.getOrElse("_id").split(",") :+ "_id"
    val takeFieldsParam: Array[String] = takeFields.getOrElse(identifierField).split(",") :+ identifierField

    val docsListOneDiffDocsListTwo: Seq[Document] = compareDocumentsBetweenLists(docs_list1, docs_list2, identifierField, takeFieldsParam)
    val docsListTwoDiffDocsListOne: Seq[Document] = compareDocumentsBetweenLists(docs_list2, docs_list1, identifierField, takeFieldsParam)

    val docsListOneValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo, identifierField)
    val listDocsOneWithoutMongoId: Seq[Document] = docsListOneValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    val docsListTwoValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListTwoDiffDocsListOne, identifierField)
    val listDocsTwoWithoutMongoId: Seq[Document] = docsListTwoValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    compareDocuments(listDocsOneWithoutMongoId, listDocsTwoWithoutMongoId, takeFieldsParam, identifierField)
  }

  private def compareDocuments(seq1: Seq[Document], seq2: Seq[Document], takeFieldsParam: Array[String], identifierField: String): Seq[Array[(String, AnyRef)]] = {

    val keyUpdd: String = "_updd"
    val keyUpddSrc: String = "_upddSrc"

    val resultSeq = for {
      doc1 <- seq1

      valueIdDoc1 = doc1.filterKeys(_.equals(identifierField)).values.head.asString().getValue
      doc2: Document = seq2.find(doc => doc.contains(identifierField) && doc.containsValue(valueIdDoc1)).getOrElse(Document())

      if doc1.get(identifierField) == doc2.get(identifierField)
      keys = doc1.keySet.intersect(doc2.keySet) ++ doc1.keySet.diff(doc2.keySet) ++ doc2.keySet.diff(doc1.keySet)
      result = keys.flatMap(key => {
        if (key != identifierField && key != keyUpdd && key != keyUpddSrc) {
          val value1: (String, AnyRef) = checkIsArrayOrStringOrDate(doc1, doc2, key)
          val value2: (String, AnyRef) = checkIsArrayOrStringOrDate(doc2, doc1, key)
          if (value1._2 != value2._2 || takeFieldsParam.contains(value1._1))
            Some((key, Array(value1._2, value2._2, "Diff: ".concat(StringUtils.difference(value1._2.toString, value2._2.toString))))) else None
        } else None
      })
      if result.nonEmpty
    } yield (identifierField, doc1.get(identifierField).get.asString().getValue) +: result.toArray

    resultSeq.filter(_.nonEmpty).map(f => f.map(h => (h._1, h._2)))
  }

  private def checkIsArrayOrStringOrDate(doc: Document, docCompare: Document, key: String): (String, AnyRef) = {

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
    } else if (docAllFields.get(key).get.isDateTime) (key, docAllFields.get(key).get.asDateTime().getValue.toString)
    else (key, docAllFields.get(key).get.asString().getValue)
  }

  private def isValueArray(key: String, docAllFields: Document): Boolean = {
    docAllFields.get(key).get.getBsonType.name() == "ARRAY"
  }

  private def deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo: Seq[Document], identifierField: String): Seq[Document] = {
    docsListOneDiffDocsListTwo.filterNot(f => f.isEmpty || f.contains("_id") && f.size == 1 || f.contains("_id") && f.contains(identifierField) && f.size <= 2)
  }

  private def compareDocumentsBetweenLists(list1: Seq[Document], list2: Seq[Document], identifierField: String, takeFieldsParam: Array[String]): Seq[Document] = {
    val batchSize: Int = 25000
    val docsCompared = new mutable.ArrayBuffer[Document]

    val list1Grouped: Iterator[Seq[Document]] = list1.grouped(batchSize)
    val list2Grouped: Iterator[Seq[Document]] = list2.grouped(batchSize)

    val listPairs: Iterator[(Seq[Document], Seq[Document])] = list1Grouped.zipAll(list2Grouped, Seq.empty, Seq.empty)

    for ((batchList1, batchList2) <- listPairs) {
      val docsInBatch = batchList1.map { doc1 =>
        val valueIDdoc1: BsonValue = fromSpecific(doc1).get(identifierField).get
        val doc2: Option[Document] = batchList2.find(_.getString(identifierField).equals(valueIDdoc1.asString().getValue))
        val docResult: IterableOnce[(String, BsonValue)] = doc2 match {
          case Some(doc2) =>
            doc1.filter { h =>
              !doc2.exists(g =>
                if (takeFieldsParam.contains(h._1)) {
                  false
                } else {
                  if (h._1.equals(g._1))
                    h._2.equals(g._2)
                  else false
                })
            }
          case None => None
        }
        doc1.filter(f => docResult.iterator.exists(h => f.equals(h)))
      }
      docsCompared ++= docsInBatch
    }
    docsCompared.toSeq
  }

  private def updateField_updd(dataListFinal: Seq[Array[(String, AnyRef)]], noUpDate: Boolean): Array[Array[(String, AnyRef)]] = {

    val dataListFinalRefactored = if (noUpDate) {
      dataListFinal
    } else {
      val dataWithNewUpdd = dataListFinal.map(f => f.filterNot(h => h._1 == "_updd"))
      dataWithNewUpdd.map(f => f.appended("_updd", new Date().toString))
    }
    dataListFinalRefactored.toArray
  }
}