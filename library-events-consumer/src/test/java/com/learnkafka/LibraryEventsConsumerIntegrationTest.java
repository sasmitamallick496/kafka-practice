package com.learnkafka;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.isA;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
								  "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endPointRegistry;
	
	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;
	
	@SpyBean
	LibraryEventService libraryEventServiceSpy;
	
	@Autowired
	LibraryEventRepository libraryEventRepository;
	
	@Autowired
	ObjectMapper objectMapper; 
	
	
	@BeforeEach
	void setUp() {
		endPointRegistry.getAllListenerContainers().forEach(messageListenerContainer -> {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		});
		
	}
	
	@AfterEach
	void tearDown() {
		libraryEventRepository.deleteAll();
	}
	
	@Test
	public void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":111,\"bookName\":\"Springboot with Kafka\",\"bookAuthor\":\"Sasmita\"}}";
		kafkaTemplate.sendDefault(json).get();
		
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		//verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
		//verify(libraryEventServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventRepository.findAll();
		
		assert libraryEventList.size() == 1;
		libraryEventList.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(111, libraryEvent.getBook().getBookId());
		});
	}
	
	public void publishUpdateLibrartyEvent() throws JsonMappingException, JsonProcessingException, InterruptedException {
		//given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":111,\"bookName\":\"Springboot with Kafka\",\"bookAuthor\":\"Sasmita\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		
		//then
		Book updatedBook = Book.builder().bookId(111).bookName("Springboot with Kafka 2.x").bookAuthor("Sasmita").build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		String updatedJson = objectMapper.writeValueAsString(libraryEvent);
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson);
		
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		//then
		verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		
		LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals("Springboot with Kafka 2.x", persistedLibraryEvent.getBook().getBookName());
		
		
		
		
	}

}
