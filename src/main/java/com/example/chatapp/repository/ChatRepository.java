package com.example.chatapp.repository;

import com.example.chatapp.model.ChatMessage;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ChatRepository extends ReactiveMongoRepository<ChatMessage, String> {

    @Tailable
    Flux<ChatMessage> findWithTailableCursorBy();
}
