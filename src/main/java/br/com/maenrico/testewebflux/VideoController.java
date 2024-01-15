package br.com.maenrico.testewebflux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.IOException;

@RestController
public class VideoController {

    @Autowired
    private StreamingService streamingService;

    private static final Logger log = LoggerFactory.getLogger(StorageComponent.class);

    private StorageComponent storageComponent;

    public VideoController() throws IOException {
        this.storageComponent = new StorageComponent("netflixo-videos", "netflixo-410521");
    }

    @GetMapping(value = "/videos/{uuid}", produces = "video/mp4")
    public Flux<DataBuffer> streamVideo(@PathVariable String uuid,
                                        @RequestHeader("Range") String range,
                                        ServerHttpRequest request,
                                        ServerHttpResponse response) {
        try {
            String targetName = String.format("%s.mp4", uuid);

            // Configura os headers da resposta



            // Realiza o streaming do vídeo
            return storageComponent.downloadFileStreaming(uuid, request, response);
        } catch (Exception e) {
            // Trate a exceção conforme necessário
            log.error("Erro ao fazer streaming do vídeo", e);
            return Flux.error(e);
        }
    }
}