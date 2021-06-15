package com.undertone.udms.logsagg.utils

import java.net.URLDecoder

import com.amazonaws.services.s3.event.S3EventNotification
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.{BatchResultErrorEntry, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, DeleteMessageBatchResult, Message, ReceiveMessageRequest}
import com.undertone.udmp.utils.HasLogger

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON

class SqsQueueHandler(queue_url: String) extends HasLogger{
  private val MAX_NUM_OF_MESSAGES = 10
  val sqs: AmazonSQS = AmazonSQSClientBuilder.defaultClient()
  val deleteMessageBatchRequestEntries: ListBuffer[DeleteMessageBatchRequestEntry] = ListBuffer[DeleteMessageBatchRequestEntry]()

  def getNewFilesData: (List[String], Long) = {
    val receiveMessageRequest: ReceiveMessageRequest = new ReceiveMessageRequest(queue_url)
    receiveMessageRequest.setMaxNumberOfMessages(MAX_NUM_OF_MESSAGES)
    var messages: List[Message] = sqs.receiveMessage(receiveMessageRequest).getMessages.toList
    var paths: ListBuffer[String] = ListBuffer[String]()
    var batchSize: Long = 0
    while (messages.nonEmpty) {
      messages.foreach(m => {
        deleteMessageBatchRequestEntries += new DeleteMessageBatchRequestEntry(m.getMessageId, m.getReceiptHandle)
        val sqsMessageBody: String = JSON.parseFull(m.getBody).get.asInstanceOf[Map[String, String]]("Message")
        val records: List[S3EventNotification.S3EventNotificationRecord] = S3EventNotification.parseJson(sqsMessageBody).getRecords.toList
        //protect the code from messages not containing the Records object
        if (records!=null) {
          for (rec <- records) {
            //take only put events
            if (rec.getEventName == "ObjectCreated:Put") {
              paths += "s3://" + rec.getS3.getBucket.getName + "/" + URLDecoder.decode(rec.getS3.getObject.getKey, "UTF8")
              batchSize += rec.getS3.getObject.getSizeAsLong.toLong
            }
          }
        }
      })
      messages = sqs.receiveMessage(receiveMessageRequest).getMessages.toList
    }
    (paths.toList, batchSize)
  }

  def deleteMessages(): List[BatchResultErrorEntry] = {
    logger.info("Removing " + deleteMessageBatchRequestEntries.size + " from queue.")
    val deleteMessageBatchRequestEntriesGrouped = deleteMessageBatchRequestEntries.grouped(MAX_NUM_OF_MESSAGES)
    val failures: List[BatchResultErrorEntry] = List[BatchResultErrorEntry]()
    for (deleteMessageBatchEntry <- deleteMessageBatchRequestEntriesGrouped){
      val deleteMessageBatchRequest: DeleteMessageBatchRequest = new DeleteMessageBatchRequest(queue_url, deleteMessageBatchEntry)
      val result: DeleteMessageBatchResult = sqs.deleteMessageBatch(deleteMessageBatchRequest)
      failures ++ result.getFailed.toList
    }
    failures
  }

  def deleteMessages(deleteMessageBatchRequestEntries: ListBuffer[DeleteMessageBatchRequestEntry]): List[BatchResultErrorEntry] = {
    logger.info("Removing " + deleteMessageBatchRequestEntries.size + "from queue")
    val deleteMessageBatchRequestEntriesGrouped = deleteMessageBatchRequestEntries.grouped(MAX_NUM_OF_MESSAGES)
    val failures: List[BatchResultErrorEntry] = List[BatchResultErrorEntry]()
    for (deleteMessageBatchEntry <- deleteMessageBatchRequestEntriesGrouped){
      val deleteMessageBatchRequest: DeleteMessageBatchRequest = new DeleteMessageBatchRequest(queue_url, deleteMessageBatchEntry)
      val result: DeleteMessageBatchResult = sqs.deleteMessageBatch(deleteMessageBatchRequest)
      failures ++ result.getFailed.toList
    }
    failures
  }

}
