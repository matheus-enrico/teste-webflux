package br.com.maenrico.testewebflux;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class GCSUploader {

    private static final String BUCKET_NAME = "seu-bucket";
    private static final String OBJECT_NAME = "nome-do-video.mp4";
    private static final int CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

    public static void main(String[] args) {
        try {
            // Carregar credenciais do arquivo JSON
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("caminho-para-seu-arquivo-json-de-credenciais"));
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

            // Carregar o vídeo local
            File videoFile = new File("caminho-para-seu-video-local");

            // Criar o objeto BlobInfo
            BlobInfo blobInfo = BlobInfo.newBuilder(BUCKET_NAME, OBJECT_NAME).build();

            // Criar um canal para o arquivo
            try (FileChannel fileChannel = new FileInputStream(videoFile).getChannel()) {
                ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
                byte[] bytes;

                // Ler e fazer upload dos chunks
                while (fileChannel.read(buffer) > 0) {
                    buffer.flip();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    buffer.clear();

                    // Fazer upload do chunk para o Google Cloud Storage
                    BlobId blobId = BlobId.of(BUCKET_NAME, OBJECT_NAME);
                    Blob blob = storage.create(blobInfo, bytes);

                    System.out.println("Chunk enviado: " + blob.getSize() + " bytes");
                }

                System.out.println("Upload concluído!");
            }
        } catch (IOException | StorageException e) {
            e.printStackTrace();
        }
    }
}
