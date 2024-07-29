package edu.kpi.task;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ConversionPlayground {

    private static final String PATH_TO_FILE = "/home/rd/Documents/training/llm/converter/document-converter/files/Shekhovtsov_bakalavr.pdf";
    private static final String WORKING_DIRECTORY = "/home/rd/Documents/training/llm/converter/document-converter/files/working/";

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

    public static void main(String[] args) throws Exception {

        copyIntoWorkingDirectory();
        executeStructAnalysis();

        final String structuredContent = getStructuredContent();

        if (!structuredContent.isEmpty()) {

            final String strippedContent = stripContentToTableOfContents(structuredContent);

            final List<Integer> topLevelHeaderIndexes = getTopLevelHeaderIndexes(strippedContent);

            final Map<String, String> chapters = getChapters(strippedContent, topLevelHeaderIndexes);

            chapters.keySet().stream().forEach(System.out::println);
        }

        FileUtils.deleteDirectory(new File(WORKING_DIRECTORY));
    }

    private static void copyIntoWorkingDirectory() throws Exception {

        Path sourcePath = Paths.get(PATH_TO_FILE);
        Path targetPath = Paths.get(WORKING_DIRECTORY + SOURCE_FILE_NAME);

        Files.createDirectories(targetPath.getParent());

        final FileChannel sourceChannel = new FileInputStream(sourcePath.toFile()).getChannel();
        final FileChannel targetChannel = new FileOutputStream(targetPath.toFile()).getChannel();

        sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
    }

    private static void executeStructAnalysis() throws Exception {

        ProcessBuilder processBuilder = new ProcessBuilder("/usr/bin/pdfinfo", "-struct-text", WORKING_DIRECTORY + SOURCE_FILE_NAME);
        processBuilder.redirectOutput(new File(WORKING_DIRECTORY + STRUCT_AND_TEXT_FILE_NAME));

        Process process = processBuilder.start();

        int exitValue = process.waitFor();

        if (exitValue != 0) {
            // Handle the case where the script execution failed
            System.out.println("Script execution failed with exit code: " + exitValue);
        }
    }

    private static String getStructuredContent() throws Exception {

        return Files.readString(Paths.get(WORKING_DIRECTORY + STRUCT_AND_TEXT_FILE_NAME));
    }

    private static String stripContentToTableOfContents(String content) throws Exception {

        final int tocIndex = content.indexOf(TABLE_OF_CONTENTS_TAG);

        return content.substring(tocIndex);
    }

    private static List<Integer> getTopLevelHeaderIndexes(String content) {

        List<Integer> result = new ArrayList<>();

        for (int index = content.indexOf(FIRST_LEVEL_HEADER);
             index >= 0;
             index = content.indexOf(FIRST_LEVEL_HEADER, index + 1)) {
            result.add(index);
        }

        return result;
    }

    private static Map<String, String> getChapters(String content, List<Integer> topLevelHeaderIndexes) {

        final List<String> chapters = IntStream.range(0, topLevelHeaderIndexes.size() - 1)
                .boxed()
                .map(i -> content.substring(topLevelHeaderIndexes.get(i), topLevelHeaderIndexes.get(i + 1)))
                .toList();

        Map<String, String> result = new LinkedHashMap<>();

        chapters.forEach(chapter -> result.put(getNameOfChapter(chapter), cleanupChapter(chapter)));

        return result;
    }

    private static String getNameOfChapter(String chapterContent) {

        final int index = chapterContent.indexOf('\n');
        final int index1 = chapterContent.indexOf('\n', index + 1);

        return chapterContent.substring(index, index1).replace("\"", "").replace("\n", "").trim();
    }

    private static String cleanupChapter(String chapter) {

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

        return result.toString();
    }
}
