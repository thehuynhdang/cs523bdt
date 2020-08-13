package kafka.producer.api;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

@RestController
public class KafkaProducerController {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private Environment env;
	@Autowired
	private Gson gson;

	@PostMapping("/kafka/produce")
	public ResponseEntity<String> postModelToKafka(@RequestBody List<CovidCaseRecordLine> records)
			throws InterruptedException, ExecutionException {
		ListenableFuture<SendResult<String, String>> result = 
				kafkaTemplate.send(env.getProperty("spring.kafka.topic"), gson.toJson(records));
		return new ResponseEntity<>(result.get().getProducerRecord().value(), HttpStatus.OK);
	}
}
