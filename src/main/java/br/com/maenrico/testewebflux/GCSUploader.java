package br.com.maenrico.testewebflux;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class GCSUploader {

    private static final int CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

    private static final Logger log = LoggerFactory.getLogger(StorageComponent.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: GCSUploader <bucketName> <objectName>");
            return;
        }

        String bucketName = args[0];
        String objectName = args[1];

        try {
            // Load credentials from JSON file
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("path-to-your-json-credentials-file"));
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

            // Load local video
            File videoFile = new File("path-to-your-local-video");

            // Create BlobInfo object
            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).build();

            // Create a writer for the blob
            Blob blob = storage.create(blobInfo, new byte[0]); // Create an empty blob
            WriteChannel writer = blob.writer();

            // Create a channel for the file
            try (FileChannel fileChannel = new FileInputStream(videoFile).getChannel()) {
                ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
                byte[] bytes;

                // Read and upload chunks
                while (fileChannel.read(buffer) > 0) {
                    buffer.flip();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    buffer.clear();

                    // Write chunk to Google Cloud Storage
                    writer.write(ByteBuffer.wrap(bytes));

                    log.info("Chunk sent: {} bytes", bytes.length);
                }

                writer.close();

                log.info("Upload completed!");
            }
        } catch (IOException e) {
            log.error("An error occurred while uploading the file", e);
            main(args); // Tenta novamente fazer o upload
        } catch (StorageException e) {
            log.error("An error occurred while accessing Google Cloud Storage", e);
        }
    }
}