package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventService {
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	LibraryEventRepository repository;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent {} ",libraryEvent);
		switch(libraryEvent.getLibraryEventType()) {
		case NEW:
			save(libraryEvent);
			break;
		case UPDATE:
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			log.info("Invalid Library Event type");
		
		}
		
		
	}

	private void validate(LibraryEvent libraryEvent) {
		if(libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("LibraryEvent Id is missing");
		}
		Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEvent.getLibraryEventId());
		if(!libraryEventOptional.isPresent()) {
			throw new IllegalArgumentException("Not a valid Library Event");
		}
		log.info("Validation is succesfull for the library Event : {}",libraryEventOptional.get());
		
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repository.save(libraryEvent);
		log.info("Successfully Persisted the LibraryEvent {}",libraryEvent);
		
		
	}
	

}
