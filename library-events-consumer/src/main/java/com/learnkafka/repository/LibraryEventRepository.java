package com.learnkafka.repository;

import org.springframework.data.repository.CrudRepository;

import com.learnkafka.entity.LibraryEvent;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer>{
	

}
