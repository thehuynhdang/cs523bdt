package cs523.config;


public final class KafkaConfig {
	public static final String KAFKA_BROKERS = "192.168.0.147:9092";
	public static final Integer MESSAGE_COUNT = 1000;
	public static final String CLIENT_ID = "client1";
	public static final String TOPIC_NAME = "spark2-topic";
	public static final String GROUP_ID_CONFIG = "cGroup1";
	public static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
	public static final String OFFSET_RESET_LATEST = "latest";
	public static final String OFFSET_RESET_EARLIER = "earliest";
	public static final Integer MAX_POLL_RECORDS = 1;
	public static final Integer MESSAGE_SIZE = 20971520;
}
