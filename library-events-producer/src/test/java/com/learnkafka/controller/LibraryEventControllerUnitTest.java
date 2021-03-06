	package com.learnkafka.controller;

import static org.hamcrest.CoreMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
public class LibraryEventControllerUnitTest {
	
	@Autowired
	MockMvc mockMvc;
	
	@MockBean
	LibraryEventProducer libraryEventProducer;
	
	ObjectMapper objectMapper = new ObjectMapper();
	
	@Test
	public void postLibraryEvent() throws Exception {
		
		//given
		Book book  = Book.builder()
					.bookId(123)
					.bookAuthor("Sasmita")
					.bookName("SpringbootwithKafka")
					.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
									.libraryEventId(null)
									.book(book)
									.build();
		String json = objectMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent)).thenReturn(null);
		
		RequestBuilder request = MockMvcRequestBuilders.post("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON);
		
		mockMvc.perform(request).andExpect(status().isCreated()).andExpect(content().json("{libraryEventId:null,libraryEventType:NEW,book:{bookId:123,bookName:SpringbootwithKafka,bookAuthor:Sasmita}}"));
	}
	@Test
	public void postLibraryEvent_4xx() throws Exception {
		
		Book book  = Book.builder()
				.bookId(null)
				.bookAuthor("")
				.bookName("SpringbootwithKafka")
				.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
									.libraryEventId(null)
									.book(book)
									.build();
		String json = objectMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent)).thenReturn(null);
		
		RequestBuilder request = MockMvcRequestBuilders.post("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON);
		
		String expectedResult = "book.bookAuthor - must not be blank, book.bookId - must not be null";
		mockMvc.perform(request).andExpect(status().is4xxClientError())
								.andExpect(content().string(expectedResult));
	}
	
	@Test
	public void putLibraryEvent_Success() throws Exception {
		
		//given
		Book book  = Book.builder()
					.bookId(123)
					.bookAuthor("Sasmita")
					.bookName("SpringbootwithKafka")
					.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
									.libraryEventId(111)
									.libraryEventType(LibraryEventType.UPDATE)
									.book(book)
									.build();
		String json = objectMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent)).thenReturn(null);
		
		RequestBuilder request = MockMvcRequestBuilders.put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON);
		
		mockMvc.perform(request).andExpect(status().isOk()).andExpect(content().json("{libraryEventId:111,libraryEventType:UPDATE,book:{bookId:123,bookName:SpringbootwithKafka,bookAuthor:Sasmita}}"));
	}
	
	@Test
	public void putLibraryEvent_badRequest() throws Exception {
		
		Book book  = Book.builder()
				.bookId(123)
				.bookAuthor("Sasmita")
				.bookName("SpringbootwithKafka")
				.build();
		LibraryEvent libraryEvent  = LibraryEvent.builder()
									.libraryEventId(null)
									.libraryEventType(LibraryEventType.UPDATE)
									.book(book)
									.build();
		String json = objectMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent)).thenReturn(null);
		
		RequestBuilder request = MockMvcRequestBuilders.put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON);
		
		String expectedResult = "Please provide libraryEventId";
		mockMvc.perform(request).andExpect(status().isBadRequest())
								.andExpect(content().string(expectedResult));
	}
		

}
