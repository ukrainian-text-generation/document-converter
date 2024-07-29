package edu.kpi.task;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class PlainTextDocumentConversionTask implements Runnable {

    private final String BUCKET_URL_ATTR = "bucketUrl";
    private final String CONTENT_ATTR = "content";

    private final CloudStorageFileSystem sourceFileSystem;
    private final CloudStorageFileSystem targetFileSystem;
    private final DocumentReference documentReference;
    private final int retries;

    public PlainTextDocumentConversionTask(CloudStorageFileSystem sourceFileSystem, CloudStorageFileSystem targetFileSystem,
                                           DocumentReference documentReference, int retries) {

        this.sourceFileSystem = sourceFileSystem;
        this.targetFileSystem = targetFileSystem;
        this.documentReference = documentReference;
        this.retries = retries;
    }

    @Override
    public void run() {

        logRun();
        int usedAttempts = 1;
        boolean success = false;

        while (!success && usedAttempts <= retries) {

            try {

                convertDocument(documentReference);
                success = true;
                logFinished();

            } catch (Exception e) {

                System.err.print(e);
                usedAttempts++;
                logRetry();
            }
        }

    }

    private void convertDocument(DocumentReference documentReference) throws ExecutionException, InterruptedException {

        final DocumentSnapshot documentSnapshot = documentReference.get().get();

        final String bucketUrl = documentSnapshot.getString(BUCKET_URL_ATTR);

        final String sourcePath = bucketUrl.substring(bucketUrl.lastIndexOf(sourceFileSystem.bucket()) + sourceFileSystem.bucket().length());

        final int lastSlash = bucketUrl.lastIndexOf('/');
        final String fileName = bucketUrl.substring(lastSlash);
        final String filePath = fileName.substring(0, fileName.lastIndexOf('.')) + ".txt";

        try (
                final InputStream source = Channels.newInputStream(FileChannel.open(sourceFileSystem.getPath(sourcePath), StandardOpenOption.READ));
                final FileChannel outputChannel = FileChannel.open(targetFileSystem.getPath(filePath), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ContentHandler handler = new BodyContentHandler(-1);
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();

            parser.parse(source, handler, metadata);

            String content = handler.toString();

            final ApiFuture<WriteResult> documentUpdateFuture = documentReference.update(Map.of(CONTENT_ATTR, content));
            outputChannel.write(ByteBuffer.wrap(content.getBytes()));
            documentUpdateFuture.get();

        } catch (IOException | TikaException | SAXException e) {

            throw new RuntimeException(e);
        }
    }

    private void logRun() {

        System.out.println("Run task for " + documentReference.getId());
    }

    private void logFinished() {

        System.out.println("Finished task for " + documentReference.getId());
    }

    private void logRetry() {

        System.out.println("Retrying task for " + documentReference.getId());
    }
}
