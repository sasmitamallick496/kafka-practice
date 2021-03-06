package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.domain.Topic1;
import com.learnkafka.domain.Topic2;
import com.learnkafka.domain.Topic3;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException{
		//libraryEventProducer.sendLibraryEvent(libraryEvent);
		//SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		//log.info("send result is {}",sendResult.toString());
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException{
		if(libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please provide libraryEventId");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
	
	@PostMapping("/topic1")
	public ResponseEntity<Topic1> postTopic1(@RequestBody Topic1 topic1) throws JsonProcessingException, InterruptedException, ExecutionException{
		libraryEventProducer.send_Topic1(topic1);
		return ResponseEntity.status(HttpStatus.CREATED).body(topic1);
	}
	
	@PostMapping("/topic2")
	public ResponseEntity<Topic2> postTopic2(@RequestBody Topic2 topic2) throws JsonProcessingException, InterruptedException, ExecutionException{
		libraryEventProducer.send_Topic2(topic2);
		return ResponseEntity.status(HttpStatus.CREATED).body(topic2);
	}
	
	@PostMapping("/topic3")
	public ResponseEntity<Topic3> postTopic3(@RequestBody Topic3 topic3) throws JsonProcessingException, InterruptedException, ExecutionException{
		libraryEventProducer.send_Topic3(topic3);
		return ResponseEntity.status(HttpStatus.CREATED).body(topic3);
	}

}
