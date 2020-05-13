package spark.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import spark.model.data.InstObj;
import spark.model.data.PredObj;

import java.io.Serializable;
import java.util.*;

public class ModelMain implements Serializable {
    private String appName;
    private String bootstrap;
    private String inputTopic;
    private String outputTopic;
    private int interval;

    public ModelMain(String appName, String bootstrap, String inputTopic, String outputTopic, int interval) {
        this.appName = appName;
        this.bootstrap = bootstrap;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.interval = interval;
    }

    public void run() {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(interval));

        Collection<String> topics = Arrays.asList(inputTopic);

        /* Input Step */
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, setKafkaConfig(bootstrap)));
        JavaDStream<String> inputs = kafkaStream.map(ConsumerRecord::value);

        /* Inference Step */
        JavaDStream<String> files = inputs.map(input -> {
            if(!input.equals(null)) {
                ObjectMapper objectMapper = new ObjectMapper();
                InstObj instObj = objectMapper.readValue(input, InstObj.class);
                PredObj predObj = new PredObj();
                Inference inference = new Inference("/home/hjmoon/models/mnist/1");

                predObj.setPredictions(inference.execute(instObj.getInstances(), "input:0", "output/Softmax:0"));
                predObj.setInputTime(instObj.getInputTime());
                predObj.setOutputTime(System.currentTimeMillis());
                predObj.setNumber(instObj.getNumber());

                String output = objectMapper.writeValueAsString(predObj);
                return output;
            }
            return null;
        });

        /* Output Step */
        files.foreachRDD(result -> {
            result.foreach(tokafka -> {
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(setKafkaProducer(bootstrap));
                System.out.println(tokafka);
                producer.send(new ProducerRecord<String, String>(outputTopic, tokafka));
                producer.close();
            });
        });
        files.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /* setting config of kafka consumer */
    public Map<String, Object> setKafkaConfig(String bootstrap) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put("group.id", "test-group");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", false);
        return properties;
    }

    /* setting config of kafka producer */
    public Properties setKafkaProducer(String bootstrap) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public static void main(String[] args) {
        String bootstrap = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";
        String appName = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];
        int interval = Integer.parseInt(args[3]);

        ModelMain modelMain = new ModelMain(appName, bootstrap, inputTopic, outputTopic, interval);
        modelMain.run();
    }
}
