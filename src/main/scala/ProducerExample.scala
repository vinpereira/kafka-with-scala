import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by vinicius on 26/04/17.
  */
class ProducerExample(val brokers: String,
                      val topic: String) extends Logging {

  val props: Properties = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, String](props)
  val executor: ExecutorService = null

  def createProducerConfig(brokers: String): Properties = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    props
  }

  def shutdown(): Unit = {
    if (producer != null)
      producer.close()
    if (executor != null)
      executor.shutdown()
  }

  def run(): Unit = {
    Executors.newSingleThreadExecutor().execute(() => {
      while (true) {
        val input = io.StdIn.readLine()

        val data = new ProducerRecord[String, String](topic, null, input)

        producer.send(data)
      }
    })
  }

}

object ProducerExample extends App {
  val example = new ProducerExample("localhost:9092", "test")
  example.run()
}
