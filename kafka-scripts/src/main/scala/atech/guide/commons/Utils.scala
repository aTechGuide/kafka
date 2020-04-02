package atech.guide.commons

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Utils {

  private val kafkaBootstrapServer = "localhost:9092"

  private val logger = LoggerFactory.getLogger(getClass)

  def getProducerProperties: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  def getConsumerProperties(groupID: String, offset: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset)
    props
  }

  def fetchMetadataFromPromise(promise: Promise[RecordMetadata]): Unit = {
    val future = promise.future
    future.onComplete({
      case Success(metadata) => logger.info(s"Topic = ${metadata.topic()}, Partition = ${metadata.partition}, Offset = ${metadata.offset}")
      case Failure(exception) => logger.error(s"Received error ${exception.getMessage}")
    })
  }

  def createCallback(promise: Promise[RecordMetadata]) = {
    new Callback() {
      // Executes every time record is successfully sent or an exception is thrown
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {

        Option(exception) match {
          case Some(err) => promise.failure(err)
          case None => promise.success(metadata)
        }
      }
    }
  }
}
