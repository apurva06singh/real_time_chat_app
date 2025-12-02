package com.example.chatapp.handler;

import com.example.chatapp.model.ChatMessage;
import com.example.chatapp.repository.ChatRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler implements WebSocketHandler {

    private final ChatRepository chatRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Sink to broadcast messages to all subscribers
    private final Sinks.Many<ChatMessage> chatSink = Sinks.many().multicast().onBackpressureBuffer();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        // Input stream: Receive messages from client, save to DB, and push to Sink
        Flux<Void> input = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(payload -> {
                    try {
                        ChatMessage message = objectMapper.readValue(payload, ChatMessage.class);
                        message.setTimestamp(LocalDateTime.now());
                        return chatRepository.save(message)
                                .doOnNext(chatSink::tryEmitNext)
                                .then();
                    } catch (JsonProcessingException e) {
                        return Mono.error(e);
                    }
                });

        // Output stream: Send messages from Sink to client
        Flux<WebSocketMessage> output = chatSink.asFlux()
                .flatMap(message -> {
                    try {
                        String json = objectMapper.writeValueAsString(message);
                        return Mono.just(session.textMessage(json));
                    } catch (JsonProcessingException e) {
                        return Mono.error(e);
                    }
                });

        return Mono.zip(input.then(), session.send(output).then()).then();
    }
}
