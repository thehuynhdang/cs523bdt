package cs523.kafka;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cs523.config.KafkaConfig;
import cs523.hbase.CasesByDateDatatable;
import cs523.hbase.FullCovid19DataTable;
import cs523.model.Covid19Row;
import cs523.model.Covid19Row.Covid19RowBuilder;
import cs523.model.SumCasesByDateRow;
import cs523.sparksql.SparkSQL;

public class KafkaConsumer {
	public static void main(String[] args) throws InterruptedException {
		//will move those into properties file
	 	String brokers = KafkaConfig.KAFKA_BROKERS;
	    String groupId = KafkaConfig.GROUP_ID_CONFIG;
	    String topics = KafkaConfig.TOPIC_NAME;

	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setAppName("KafkaConsumer").setMaster("local");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    Map<String, Object> kafkaParams = new HashMap<>();
	    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
	    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
	    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

	    // Create direct kafka stream with brokers and topics
	    JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
	        jssc,
	        LocationStrategies.PreferConsistent(),
	        ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
	    
	    stream.map(f -> f.value()).foreachRDD(p -> {
	    	if(p != null && !p.isEmpty()) {
	    		System.out.println("##########: FullCovid19DataTable begin saving" + p.first());
	    		
		    	//saving RDD into HBase here
	    		Gson gson = new Gson();
	    		Type listOfMyClassObject = new TypeToken<ArrayList<Covid19RowBuilder>>(){}.getType();
	    	    List<Covid19RowBuilder> covid19Rows = gson.fromJson(p.first(), listOfMyClassObject);
	    	    List<Covid19Row> rows = covid19Rows.stream().map(
	    	    		row -> row.build()).collect(Collectors.toList());
	    	    
	    	    new FullCovid19DataTable(rows).persist();
	    	    System.out.println("##########: FullCovid19DataTable end saving" + covid19Rows);
	    	    
	    	    //also, need to update summary in HBase for Jupyter Chart
	    	    System.out.println("##########: CasesByDateDatatable updating statics...");
	    	    SparkSQL sparkSQL = new SparkSQL();
	    	    List<SumCasesByDateRow> updates = sparkSQL.summarizeCasesByDate();
	    	    rows = updates.stream().map(row -> row.toCovid19Row()).collect(Collectors.toList());
	    	    
		    	new CasesByDateDatatable(rows).persist();
	    	    //sparkSQL.closeSession();
	    	    System.out.println("##########: CasesByDateDatatable end updates count " + updates.size());
	    	}
	    });
	    
	    // Start the computation
	    jssc.start();
		jssc.awaitTermination();
	}
}
