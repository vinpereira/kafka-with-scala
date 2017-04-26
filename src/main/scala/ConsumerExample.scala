import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutorService, Executors}

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

/**
  * Created by vinicius on 26/04/17.
  */
class ConsumerExample(val brokers: String,
                      val topic: String) extends Logging {

  val props: Properties = createConsumerConfig(brokers)
  val consumer = new KafkaConsumer[String, String](props)
  val executor: ExecutorService = null

  def createConsumerConfig(brokers: String): Properties = {
    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
    //    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    //    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    props
  }

  def shutdown(): Unit = {
    if (consumer != null)
      consumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def run(): Unit = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor().execute(() => {
      while (true) {
        val records = consumer.poll(1000).asScala

        for (record <- records) {
          System.out.println("Received message: " + record.value())
        }
      }
    })
  }

}

object ConsumerExample extends App {
  val example = new ConsumerExample("localhost:9092", "test")
  example.run()
}
