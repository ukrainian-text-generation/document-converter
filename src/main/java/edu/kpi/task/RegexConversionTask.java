package edu.kpi.task;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import edu.kpi.stripper.ExcludeHeaderFooterTextStripper;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RegexConversionTask implements Runnable {

    private static final String WORKING_DIRECTORY_TEMPLATE = "~/working/%s/";
    private static final String COLLECTIONS_ATTRIBUTE = "collections";
    private static final List<String> ALLOWED_COLLECTIONS = List.of("Бакалаврські роботи", "Магістерські роботи");
    private static final String BUCKET_URL_ATTR = "bucketUrl";
    private static final String CHAPTERS_ATTR = "chapters";
    private static final String SOURCE_FILE_NAME = "source.pdf";

    private static final Pattern DOCUMENT_START_PATTERN = Pattern.compile("^\\s*[0-9.]*\\s*(РЕФЕРАТ|АНОТАЦІЯ)\\s*$", Pattern.MULTILINE);
    private static final Pattern DOCUMENT_END_PATTERN = Pattern.compile("^[0-9.]*\\s*(СПИСОК\\s+(ЛІТЕРАТУРИ|.*ДЖЕРЕЛ)|ПЕРЕЛІК\\s+(ПОСИЛАНЬ|.*ДЖЕРЕЛ)|ВИКОРИСТАН.*ЛІТЕРАТУРА|ДЖЕРЕЛА)\\s*$", Pattern.MULTILINE);
    private static final Pattern HEADER_PATTERN = Pattern.compile("^\\s*[0-9.]*\\s*([А-ЯІЄЇ]{5,}[А-ЯІЄЇ, \n]*)\\s*$", Pattern.MULTILINE);
    private static final String SPLIT_HEADER_REGEX = "([А-ЯІЄЇ, ]{30,})(\n)([А-ЯІЄЇ, ]{5,})";
    private static final String SPLIT_HEADER_REPLACEMENT = "$1 $3";
    private static final String PDF_EXTENSION = ".pdf";

    private final CloudStorageFileSystem sourceFileSystem;
    private final DocumentReference documentReference;
    private final int retries;
    private final String workingDirectory;

    public RegexConversionTask(CloudStorageFileSystem sourceFileSystem, DocumentReference documentReference, int retries) {
        this.sourceFileSystem = sourceFileSystem;
        this.documentReference = documentReference;
        this.retries = retries;
        this.workingDirectory = WORKING_DIRECTORY_TEMPLATE.formatted(documentReference.getId());
    }

    @Override
    public void run() {

        logRun();
        int usedAttempts = 1;
        boolean success = false;

        while (!success && usedAttempts <= retries) {

            try {

                convertDocument();
                success = true;

            } catch (Exception e) {

                System.err.print(e);
                usedAttempts++;
                logRetry();
            }
        }

    }

    @SneakyThrows
    public void convertDocument() {

        if (copyIntoWorkingDirectory()) {

            logStarting();

            final String text =
                    joinSplitHeaders(
                            cleanUpBreaks(
                                    cropToEnd(
                                            cropToStart(
                                                    parsePdfToText()))));

            documentReference.update(Map.of(CHAPTERS_ATTR, getChapters(text, getHeaders(text))))
                    .get();

            FileUtils.deleteDirectory(new File(workingDirectory));

            logFinished();

        }
    }

    @SneakyThrows
    private boolean copyIntoWorkingDirectory() {

        final DocumentSnapshot documentSnapshot = documentReference.get().get();

        if (documentSnapshot.get(CHAPTERS_ATTR) != null) return false;

        return Optional.ofNullable(documentSnapshot.get(COLLECTIONS_ATTRIBUTE))
                .filter(List.class::isInstance)
                .map(collections -> (List<String>) collections)
                .filter(this::isInAllowedCollection)
                .map(collections -> copyIntoWorkingDirectory(documentSnapshot))
                .orElse(Boolean.FALSE);
    }

    @SneakyThrows
    private boolean copyIntoWorkingDirectory(final DocumentSnapshot documentSnapshot) {

        final String bucketUrl = documentSnapshot.getString(BUCKET_URL_ATTR);

        if (bucketUrl == null || !bucketUrl.endsWith(PDF_EXTENSION)) return false;

        final String sourcePath = bucketUrl.substring(bucketUrl.lastIndexOf(sourceFileSystem.bucket()) + sourceFileSystem.bucket().length());

        Path targetPath = Paths.get(workingDirectory + SOURCE_FILE_NAME);

        Files.createDirectories(targetPath.getParent());

        try (final FileChannel sourceChannel = FileChannel.open(sourceFileSystem.getPath(sourcePath), StandardOpenOption.READ);
             final FileChannel targetChannel = new FileOutputStream(targetPath.toFile()).getChannel()) {

            sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
        }

        return true;
    }

    private boolean isInAllowedCollection(final List<String> collections) {

        return collections.stream()
                .anyMatch(ALLOWED_COLLECTIONS::contains);
    }

    @SneakyThrows
    private String parsePdfToText() {

        final PDDocument document = PDDocument.load(new File(workingDirectory + SOURCE_FILE_NAME));

        final PDFTextStripper stripper = new ExcludeHeaderFooterTextStripper();
        final String text = stripper.getText(document);

        document.close();

        return text;
    }

    private String cropToStart(final String text) {

        final Matcher matcher = DOCUMENT_START_PATTERN.matcher(text);

        if (matcher.find()) {

            return text.substring(matcher.start());

        } else {

            throw new IllegalArgumentException("No document start found");
        }
    }

    private String cropToEnd(final String text) {

        final Matcher matcher = DOCUMENT_END_PATTERN.matcher(text);
        int lastMatchIndex = 0;

        while (matcher.find()) {

            lastMatchIndex = matcher.end();
        }

        return text.substring(0, lastMatchIndex);
    }

    private String cleanUpBreaks(final String text) {

        return text.replaceAll("(\\s\n)+", "\n");
    }

    private String joinSplitHeaders(final String text) {

        return text.replaceAll(SPLIT_HEADER_REGEX, SPLIT_HEADER_REPLACEMENT);
    }

    private Map<String, Integer> getHeaders(final String text) {

        final Map<String, Integer> result = new LinkedHashMap<>();

        final Matcher matcher = HEADER_PATTERN.matcher(text);

        while (matcher.find()) {

            result.put(matcher.group().replace("\n", "").trim(), matcher.start());
        }

        return result;
    }

    private Map<String, String> getChapters(final String text, final Map<String, Integer> headers) {

        final List<String> headerList = new ArrayList<>(headers.keySet());
        final List<Integer> headerIndexes = new ArrayList<>(headers.values());

        return IntStream.range(0, headerIndexes.size() - 1)
                .boxed()
                .map(chapterIndex -> Map.entry(
                        headerList.get(chapterIndex),
                        cleanUpChapterText(text.substring(headerIndexes.get(chapterIndex), headerIndexes.get(chapterIndex + 1)))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
    }

    private String cleanUpChapterText(final String text) {

        return text.replace("\n", " ");
    }

    private void logRun() {

        logMessage("Run task for " + documentReference.getId());
    }

    private void logFinished() {

        logMessage("Finished task for " + documentReference.getId());
    }

    private void logRetry() {

        logMessage("Retrying task for " + documentReference.getId());
    }

    private void logStarting() {

        logMessage("Starting task for " + documentReference.getId());
    }

    private void logMessage(final String message) {

        System.out.println(message);
    }
}
