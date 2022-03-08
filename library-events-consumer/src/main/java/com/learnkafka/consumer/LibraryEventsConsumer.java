package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learnkafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsConsumer {
	@Autowired
	LibraryEventService libraryEventService;
	
	@KafkaListener(topics = {"Library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		log.info("Consumer Record : {}",consumerRecord);
		libraryEventService.processLibraryEvent(consumerRecord);
		
	}
	
	@KafkaListener(topics = {"Topic1"})
	public void onMessage_Topic1(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consumer Record Topic1 : {}",consumerRecord);
	}
	@KafkaListener(topics = {"Topic2"})
	public void onMessage_Topic2(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consumer Record Topic2 : {}",consumerRecord);
	}
	@KafkaListener(topics = {"Topic3"})
	public void onMessage_Topic3(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consumer Record Topic3 : {}",consumerRecord);
	}
	
	

}
