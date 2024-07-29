package edu.kpi;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.CloudStorageConfiguration;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import edu.kpi.task.RegexConversionTask;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class DocumentConverter {

    private final String sourceBucketName;
    private final String targetBucketName;
    private final String projectId;
    private final String databaseId;
    private final String collectionId;
    private final int retries;
    private final int batchSize;

    public DocumentConverter(String sourceBucketName, String targetBucketName, String projectId, String databaseId, String collectionId, int retries, int batchSize) {

        this.sourceBucketName = sourceBucketName;
        this.targetBucketName = targetBucketName;
        this.projectId = projectId;
        this.databaseId = databaseId;
        this.collectionId = collectionId;
        this.retries = retries;
        this.batchSize = batchSize;
    }

    public void convertDocuments() {

        final Firestore database = getDatabase();
        final CloudStorageFileSystem sourceFileSystem = getFilesystem(sourceBucketName);
        final CloudStorageFileSystem targetFileSystem = getFilesystem(targetBucketName);

        try (final ExecutorService executorService = Executors.newFixedThreadPool(4)) {

            final List<DocumentReference> documentReferences = StreamSupport.stream(database.collection(collectionId)
                    .listDocuments().spliterator(), Boolean.FALSE).toList();

            IntStream.range(0, documentReferences.size())
                    .boxed()
                    .collect(Collectors.groupingBy(i -> i / batchSize,
                            Collectors.mapping(documentReferences::get, Collectors.toList())))
                    .values()
                    .forEach(batch -> convertBatch(executorService, sourceFileSystem, targetFileSystem, batch));
        }
    }

    @SneakyThrows
    private void convertBatch(ExecutorService executorService, CloudStorageFileSystem sourceFileSystem, CloudStorageFileSystem targetFileSystem, Collection<DocumentReference> batch) {

        List<Future<?>> futures = new ArrayList<>();

        batch.stream()
//                .map(doc -> new PlainTextDocumentConversionTask(sourceFileSystem, targetFileSystem, doc, retries))
//                .map(doc -> new PdfInfoDocumentConversionTask(sourceFileSystem, doc, retries))
                .map(doc -> new RegexConversionTask(sourceFileSystem, doc, retries))
                .map(executorService::submit)
                .forEach(futures::add);

        for (Future<?> future : futures) future.get();
    }

    private Firestore getDatabase() {

        return FirestoreOptions.getDefaultInstance().toBuilder()
                .setProjectId(projectId)
                .setDatabaseId(databaseId)
                .build()
                .getService();
    }

    private CloudStorageFileSystem getFilesystem(String bucket) {

        return CloudStorageFileSystem.forBucket(
                bucket,
                CloudStorageConfiguration.DEFAULT,
                StorageOptions.newBuilder()
                        .build());
    }
}
