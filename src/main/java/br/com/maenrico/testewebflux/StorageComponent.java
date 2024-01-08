package br.com.maenrico.testewebflux;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StorageComponent {

    private static final Logger log = LoggerFactory.getLogger(StorageComponent.class);

    private String bucketName;

    private Storage storage;

    public StorageComponent(String bucketName, String projectName) throws IOException {
        // 7000
        // 10000 - 9600 = 400

        this.bucketName = bucketName;
        this.storage = StorageOptions.newBuilder()
                .setProjectId(projectName)
                .build()
                .getService();
    }

    public void uploadFileChunked(final File file, String targetName, int maxChunkSizeInMB) throws Exception {
        List<BlobId> blobIds = splitAndUploadChunks(file, targetName, maxChunkSizeInMB, storage);

        log.info("Composing chunks into {}", targetName);
        Storage.ComposeRequest.Builder composeBuilder = Storage.ComposeRequest.newBuilder();
        composeBuilder.setTarget(BlobInfo.newBuilder(bucketName, targetName).build());
        blobIds.forEach((blobId) -> composeBuilder.addSource(blobId.getName()));

        storage.compose(composeBuilder.build());

        //Seria interessante deletar os chunks após o compose? Ou deixar para fazer o download dos chunks e montar dps a partir deles?
        blobIds.forEach(storage::delete);
    }

    private List<BlobId> splitAndUploadChunks(final File file, String targetName, int maxChunkSizeInMB, Storage storage) throws IOException {
        List<BlobId> blobIds = new ArrayList<>();
        byte[] buffer = new byte[(1024 * 1024) * maxChunkSizeInMB]; // 1MB por chunk
        int bytesRead;
        int chunkIndex = 0;
        FileInputStream fis = new FileInputStream(file);
        while ((bytesRead = fis.read(buffer)) != -1) {
            BlobInfo blobInfo = uploadChunk(targetName, storage, chunkIndex, buffer, bytesRead);
            blobIds.add(blobInfo.getBlobId());
            chunkIndex++;
        }
        fis.close();
        return blobIds;
    }

    private BlobInfo uploadChunk(final String targetName, Storage storage, int chunkIndex, byte[] buffer, int bytesRead) {
        String chunkName = targetName + "-chunk-" + chunkIndex;
        log.info("Uploading chunk {} of size {}", chunkName, bytesRead);
        BlobId blobId = BlobId.of(bucketName, chunkName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        storage.create(blobInfo, Arrays.copyOf(buffer, bytesRead));
        log.info("Chunk {} uploaded", chunkName);
        return blobInfo;
    }

    public void downloadFileStreaming(final String uuid, String targetName) throws IOException {
        log.info("Downloading {} to {}", uuid, targetName);
        Blob blob = storage.get(BlobId.of(bucketName, String.format("%s.mp4", uuid)));
        ReadChannel reader = blob.reader();

        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024); // Tamanho do buffer, por exemplo, 64KB
        while (reader.read(buffer) > 0) {
            buffer.flip();
            // Aqui você processa os dados do buffer
            // Por exemplo, salvando em um arquivo ou transmitindo
            buffer.clear();
        }
        log.info("Downloaded {} to {}", uuid, targetName);
    }

}