package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateconfig {
	
	@Bean
	public NewTopic libraryEvents() {
		return TopicBuilder.name("library-events")
					.partitions(3)
					.replicas(3)
					.build();
	}
	
	/*@Bean
	public NewTopic createTopic1() {
		return TopicBuilder.name("Topic1")
					.partitions(3)
					.replicas(3)
					.build();
	}
	
	@Bean
	public NewTopic createTopic2() {
		return TopicBuilder.name("Topic2")
					.partitions(3)
					.replicas(3)
					.build();
	}
	@Bean
	public NewTopic createTopic3() {
		return TopicBuilder.name("Topic3")
					.partitions(3)
					.replicas(3)
					.build();
	}*/

}
