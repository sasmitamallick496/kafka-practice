package com.learnkafka.producer;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerTest {
	
	@InjectMocks
	LibraryEventProducer eventProducer;
	
	@Mock
	KafkaTemplate<Integer, String> kafkatemplate;
	
	@Spy
	ObjectMapper objectMapper;;
	
	@Test
	public void sendLibraryEvent_Approach2_failure() throws JsonProcessingException, InterruptedException, ExecutionException {
		
		Book book  = Book.builder()
				.bookId(123)
				.bookAuthor("Sasmita")
				.bookName("SpringbootwithKafka")
				.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
								.libraryEventId(null)
								.book(book)
								.build();
		SettableListenableFuture future = new SettableListenableFuture();
		future.setException(new RuntimeException("Exception calling Kafka"));
		
		when(kafkatemplate.send(new ProducerRecord<Integer, String>("", 1, ""))).thenReturn(future);
		
		assertThrows(Exception.class, () -> eventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
	}
	
	@Test
	public void sendLibraryEvent_Approach2_success() throws JsonProcessingException, InterruptedException, ExecutionException {
		
		Book book  = Book.builder()
				.bookId(123)
				.bookAuthor("Sasmita")
				.bookName("SpringbootwithKafka")
				.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
								.libraryEventId(null)
								.book(book)
								.build();
		String record = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer,String> producerRecord = new ProducerRecord<Integer, String>("library-events", libraryEvent.getLibraryEventId(), record);
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);
		SendResult<Integer,String> sendResult = new SendResult<Integer,String>(producerRecord, recordMetadata);
		SettableListenableFuture future = new SettableListenableFuture();
		future.set(sendResult);
		when(kafkatemplate.send(producerRecord)).thenReturn(future);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkatemplate.send(producerRecord);
		
		SendResult<Integer, String> sendResult1 = listenableFuture.get();
		assert sendResult1.getRecordMetadata().partition() == 1;
		
	}

}
