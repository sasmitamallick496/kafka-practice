package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.Topic1;
import com.learnkafka.domain.Topic2;
import com.learnkafka.domain.Topic3;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
	KafkaTemplate<Integer, String> kafkatemplate;

	@Autowired
	ObjectMapper objectMapper;
	
	String topic = "library-events";

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {

				handleFailure(key, value, ex);

			}
		});

	}
	
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException,InterruptedException,ExecutionException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkatemplate.sendDefault(key, value).get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("InterruptedException | ExecutionException in sending the message and the exception is {}", e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception in sending the message and the exception is {}", e.getMessage());
			throw e;
		}
		
		return sendResult;
		
	}
	
	public void send_Topic1(Topic1 topic1) throws JsonProcessingException {
		
		Integer key = topic1.getId();
		String value = objectMapper.writeValueAsString(topic1);
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key,value,"Topic1");
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {

				handleFailure(key, value, ex);

			}
		});
		

		
	}
	
public void send_Topic2(Topic2 topic2) throws JsonProcessingException {
		
		Integer key = topic2.getId();
		String value = objectMapper.writeValueAsString(topic2);
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key,value,"Topic2");
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {

				handleFailure(key, value, ex);

			}
		});
		

		
	}
public void send_Topic3(Topic3 topic3) throws JsonProcessingException {
	
	Integer key = topic3.getId();
	String value = objectMapper.writeValueAsString(topic3);
	ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key,value,"Topic3");
	ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.send(producerRecord);
	listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

		@Override
		public void onSuccess(SendResult<Integer, String> result) {
			handleSuccess(key, value, result);
		}

		@Override
		public void onFailure(Throwable ex) {

			handleFailure(key, value, ex);

		}
	});
	

	
}
public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
	
	Integer key = libraryEvent.getLibraryEventId();
	String value = objectMapper.writeValueAsString(libraryEvent);
	ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key,value,topic);
	ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.send(producerRecord);
	listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

		@Override
		public void onSuccess(SendResult<Integer, String> result) {
			handleSuccess(key, value, result);
		}

		@Override
		public void onFailure(Throwable ex) {

			handleFailure(key, value, ex);

		}
	});
	
	return listenableFuture;
	
}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
		
		List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes()));
		
		return new ProducerRecord<>(topic, null, key, value, recordHeaders);
		
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key : {} and the value is {} , partion is {}", key, value,
				result.getRecordMetadata().partition());
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error in sending the message and the exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error in onFailure: {}", throwable.getMessage());
		}

	}

}
