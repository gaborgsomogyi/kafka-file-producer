package com.kafka.producer

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.log4j.LogManager

object KafkaProducer {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      log.error("Usage: KafkaProducer [bootstrap] [topic] [filename]")
      log.error("Example: KafkaProducer localhost:9092 topic1 file.ext")
      System.exit(1)
    }

    val bootstrapServer = args(0)
    val topicName = args(1)
    val fileToSend = args(2)

    log.info("Creating config properties...")
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    log.info("OK")

    log.info("Creating kafka producer...")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    log.info("OK")

    try {
      val data = Files.readAllBytes(Paths.get(fileToSend))
      val record = new ProducerRecord[String, Array[Byte]](topicName, null, data)
      producer.send(record, onSendCallback(fileToSend))
      log.info("Record: " + record)
    } finally {
      log.info("Closing kafka producer...")
      producer.close()
      log.info("OK")
    }
  }

  def onSendCallback(fileToSend: String): Callback = new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        log.debug(s"File $fileToSend sent to topic ${metadata.topic()} in partition ${metadata.partition()} at offset ${metadata.offset()}")
      } else {
        log.error(s"Error sending file $fileToSend")
        exception.printStackTrace()
      }
    }
  }
}
