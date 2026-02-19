package it.water.connectors.kafka.model;
import it.water.core.validation.annotations.NoMalitiusCode;
public class KafkaRestMessage {
	@NoMalitiusCode
	private String key;
	@NoMalitiusCode
	private String content;
	public KafkaRestMessage(String key, String content) {
		super();
		this.key = key;
		this.content = content;
	}
	public String getKey() {
		return key;
	}
	public String getContent() {
		return content;
	}
}
