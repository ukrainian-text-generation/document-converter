package edu.kpi.task;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class PdfInfoDocumentConversionTask implements Runnable {

    private final String BUCKET_URL_ATTR = "bucketUrl";
    private final String CHAPTERS_ATTR = "chapters";
    private static final String WORKING_DIRECTORY_TEMPLATE = "~/working/%s/";

    private static final String SOURCE_FILE_NAME = "source.pdf";
    private static final String STRUCT_FILE_NAME = "struct.txt";
    private static final String STRUCT_AND_TEXT_FILE_NAME = "struct_text.txt";
    private static final String TABLE_OF_CONTENTS_TAG = "TOC";
    private static final String FIRST_LEVEL_HEADER = "H1 (block)";

    private static final Map<String, String> STRUCT_TO_MD_MAPPING = Map.ofEntries(
            Map.entry("H1 (block)", "\n# "),
            Map.entry("  H2 (block)", "\n## "),
            Map.entry("  H3 (block)", "\n### "),
            Map.entry("  P (block)", "\n"),
            Map.entry("  L (block):", ""),
            Map.entry("     /ListNumbering /None", ""),
            Map.entry("     /ListNumbering /Decimal", ""),
            Map.entry("    LI (block)", ""),
            Map.entry("      LBody (block)", "\n")
    );

    private final CloudStorageFileSystem sourceFileSystem;
    private final DocumentReference documentReference;
    private final int retries;
    private final String workingDirectory;

    public PdfInfoDocumentConversionTask(CloudStorageFileSystem sourceFileSystem, DocumentReference documentReference, int retries) {

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

    public void convertDocument() throws Exception {

        if (copyIntoWorkingDirectory()) {

            logStarting();

            executeStructAnalysis();
            executeStructTextAnalysis();

            final String structContent = getStructuredContent();
            final String structuredTextContent = getStructuredTextContent();

            if (!structuredTextContent.isEmpty() && checkHasToc(structContent)) {

                final Set<String> structTokens = getStructTokens(structContent);

                final String strippedContent = stripContentToTableOfContents(structuredTextContent);

                final List<Integer> topLevelHeaderIndexes = getTopLevelHeaderIndexes(strippedContent);

                final Map<String, String> chapters = getChapters(strippedContent, topLevelHeaderIndexes, structTokens);

                final ApiFuture<WriteResult> documentUpdateFuture = documentReference.update(Map.of(CHAPTERS_ATTR, chapters));
                documentUpdateFuture.get();
            }

            FileUtils.deleteDirectory(new File(workingDirectory));

            logFinished();

        } else {

            logSkipped();
        }
    }

    private boolean copyIntoWorkingDirectory() throws Exception {

        final DocumentSnapshot documentSnapshot = documentReference.get().get();

        final List<String> collections = (List<String>) documentSnapshot.get("collections");

        if (collections.contains("Бакалаврські роботи") || collections.contains("Магістерські роботи")) {

            final String bucketUrl = documentSnapshot.getString(BUCKET_URL_ATTR);
            final String sourcePath = bucketUrl.substring(bucketUrl.lastIndexOf(sourceFileSystem.bucket()) + sourceFileSystem.bucket().length());

            Path targetPath = Paths.get(workingDirectory + SOURCE_FILE_NAME);

            Files.createDirectories(targetPath.getParent());

            try (final FileChannel sourceChannel = FileChannel.open(sourceFileSystem.getPath(sourcePath), StandardOpenOption.READ);
                 final FileChannel targetChannel = new FileOutputStream(targetPath.toFile()).getChannel()) {

                sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
            }

            return true;
        }

        return false;
    }

    private void executeStructTextAnalysis() throws Exception {

        ProcessBuilder processBuilder = new ProcessBuilder("/usr/bin/pdfinfo", "-struct-text", workingDirectory + SOURCE_FILE_NAME);
        processBuilder.redirectOutput(new File(workingDirectory + STRUCT_AND_TEXT_FILE_NAME));

        processBuilder.start().waitFor();
    }

    private void executeStructAnalysis() throws Exception {

        ProcessBuilder processBuilder = new ProcessBuilder("/usr/bin/pdfinfo", "-struct", workingDirectory + SOURCE_FILE_NAME);
        processBuilder.redirectOutput(new File(workingDirectory + STRUCT_FILE_NAME));

        processBuilder.start().waitFor();
    }

    private boolean checkHasToc(String struct) {

        return struct.contains(TABLE_OF_CONTENTS_TAG);
    }

    private Set<String> getStructTokens(String struct) {

        return new HashSet<>(Arrays.asList(struct.split("\n")));
    }

    private String getStructuredContent() throws Exception {

        return Files.readString(Paths.get(workingDirectory + STRUCT_FILE_NAME));
    }

    private String getStructuredTextContent() throws Exception {

        return Files.readString(Paths.get(workingDirectory + STRUCT_AND_TEXT_FILE_NAME));
    }

    private String stripContentToTableOfContents(String content) {

        final int tocIndex = content.indexOf(TABLE_OF_CONTENTS_TAG);

        return content.substring(tocIndex);
    }

    private List<Integer> getTopLevelHeaderIndexes(String content) {

        List<Integer> result = new ArrayList<>();

        for (int index = content.indexOf(FIRST_LEVEL_HEADER);
             index >= 0;
             index = content.indexOf(FIRST_LEVEL_HEADER, index + 1)) {
            result.add(index);
        }

        return result;
    }

    private Map<String, String> getChapters(String content, List<Integer> topLevelHeaderIndexes, Set<String> structTokens) {

        final List<String> chapters = IntStream.range(0, topLevelHeaderIndexes.size() - 1)
                .boxed()
                .map(i -> content.substring(topLevelHeaderIndexes.get(i), topLevelHeaderIndexes.get(i + 1)))
                .toList();

        Map<String, String> result = new LinkedHashMap<>();

        chapters.forEach(chapter -> result.put(getNameOfChapter(chapter), cleanupChapter(chapter, structTokens)));

        return result;
    }

    private String getNameOfChapter(String chapterContent) {

        final int index = chapterContent.indexOf('\n');
        final int index1 = chapterContent.indexOf('\n', index + 1);

        return chapterContent.substring(index, index1).replace("\"", "").replace("\n", "").trim();
    }

    private String cleanupChapter(String chapter, Set<String> structTokens) {

        StringBuilder result = new StringBuilder();

        boolean skip = false;

        for (String line : chapter.split("\n")) {

            if (line.startsWith("  Table (block)")) {

                skip = true;

            } else if (line.lastIndexOf("  ") == 0) {

                skip = false;
            }

            if (!skip) {

                result.append(STRUCT_TO_MD_MAPPING.getOrDefault(line, line).replaceAll("(^ +\")|(\"$)", ""));
            }
        }

        String resultString = result.toString();

        for (var token : structTokens) {

            resultString = resultString.replaceAll(token, "");
        }

        return resultString;
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

    private void logStarting() {

        System.out.println("Starting task for " + documentReference.getId());
    }

    private void logSkipped() {

        System.out.println("Skipped task for " + documentReference.getId());
    }
}
