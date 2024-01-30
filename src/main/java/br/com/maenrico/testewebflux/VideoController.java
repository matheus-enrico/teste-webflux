package br.com.maenrico.testewebflux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;

@RestController
public class VideoController {

    private static final Logger log = LoggerFactory.getLogger(VideoController.class);

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
            // Realiza o streaming do vídeo
            log.info("range: {}", range);
            return storageComponent.downloadFileStreaming(uuid, response);
        } catch (Exception e) {
            // Trate a exceção conforme necessário
            log.error("Erro ao fazer streaming do vídeo", e);
            return Flux.error(e);
        }
    }

    @PostMapping("/upload")
    public ResponseEntity<?> uploadChunk(
            @RequestParam("chunk") MultipartFile chunk,
            @RequestParam("chunkIndex") int chunkIndex,
            @RequestParam("chunks") int chunks,
            @RequestParam("fileName") String fileName) throws Exception {
        File tempFile = new File("/tmp/" + fileName + "-" + chunkIndex);
        chunk.transferTo(tempFile);

        log.info("Chunk {} de {} recebido", chunkIndex, chunks);

        storageComponent.uploadFileChunked(tempFile, fileName, chunks);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}