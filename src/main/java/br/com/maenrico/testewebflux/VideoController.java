package br.com.maenrico.testewebflux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class VideoController {

    @Autowired
    private StreamingService streamingService;

    @Autowired
    private StorageComponent storageComponent;

    @GetMapping(value = "/videos/{title}", produces = "video/mp4")
    public Flux<DataBuffer> streamVideo(@PathVariable String title, ServerHttpResponse response) {
        // Obtém os chunks necessários do GCS
        List<Chunk> chunks = storageComponent.downloadFileStreaming();

        // Configura os headers da resposta
        response.getHeaders().set(HttpHeaders.CONTENT_TYPE, "video/mp4");
        response.getHeaders().set(HttpHeaders.ACCEPT_RANGES, "bytes");

        // Converte os chunks em Flux<DataBuffer> para streaming
        return Flux.fromIterable(chunks)
                .map(chunk -> {
                    // Recupera o conteúdo do chunk do GCS
                    byte[] chunkData = streamingService.getChunkData(chunk);

                    // Cria um DataBuffer a partir dos bytes do chunk
                    DataBuffer dataBuffer = response.bufferFactory().wrap(chunkData);

                    return dataBuffer;
                });
    }
}