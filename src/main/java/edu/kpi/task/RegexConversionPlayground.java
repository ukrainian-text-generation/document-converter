package edu.kpi.task;

import edu.kpi.stripper.ExcludeHeaderFooterTextStripper;
import lombok.SneakyThrows;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RegexConversionPlayground {

    private static final String PATH_TO_FILE = "/home/rd/Documents/training/llm/converter/document-converter/files/Rumiantsev_magistr.pdf";
    private static final String WORKING_DIRECTORY = "/home/rd/Documents/training/llm/converter/document-converter/files/working/";

    private static final String SOURCE_FILE_NAME = "source.pdf";
    private static final Pattern DOCUMENT_START_PATTERN = Pattern.compile("^\\s*[0-9.]*\\s*(РЕФЕРАТ|АНОТАЦІЯ)\\s*$", Pattern.MULTILINE);
    private static final Pattern DOCUMENT_END_PATTERN = Pattern.compile("^[0-9.]*\\s*(СПИСОК\\s+(ЛІТЕРАТУРИ|.*ДЖЕРЕЛ)|ПЕРЕЛІК\\s+(ПОСИЛАНЬ|.*ДЖЕРЕЛ)|ВИКОРИСТАН.*ЛІТЕРАТУРА|ДЖЕРЕЛА)\\s*$", Pattern.MULTILINE);
    private static final Pattern HEADER_PATTERN = Pattern.compile("^\\s*[0-9.]*\\s*([А-ЯІЄЇ]{5,}[А-ЯІЄЇ, \n]*)\\s*$", Pattern.MULTILINE);

    public static void main(String[] args) throws Exception {

        copyIntoWorkingDirectory();

        final String text =
                joinSplitHeaders(
                        cleanUpBreaks(
                                cropToEnd(
                                        cropToStart(
                                                parsePdfToText()))));

        System.out.println(text);

        final Map<String, Integer> headers = getHeaders(text);

        System.out.println(headers);

        final Map<String, String> chapters = getChapters(text, headers);

        System.out.println(chapters);
    }

    @SneakyThrows
    private static void copyIntoWorkingDirectory() {

        Path sourcePath = Paths.get(PATH_TO_FILE);
        Path targetPath = Paths.get(WORKING_DIRECTORY + SOURCE_FILE_NAME);

        Files.createDirectories(targetPath.getParent());

        final FileChannel sourceChannel = new FileInputStream(sourcePath.toFile()).getChannel();
        final FileChannel targetChannel = new FileOutputStream(targetPath.toFile()).getChannel();

        sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
    }

    @SneakyThrows
    private static String parsePdfToText() {

        final PDDocument document = PDDocument.load(new File(WORKING_DIRECTORY + SOURCE_FILE_NAME));

        final PDFTextStripper stripper = new ExcludeHeaderFooterTextStripper();
        final String text = stripper.getText(document);

        document.close();

        return text;
    }

    private static String cropToStart(final String text) {

        final Matcher matcher = DOCUMENT_START_PATTERN.matcher(text);

        if (matcher.find()) {

            return text.substring(matcher.start());

        } else {

            throw new IllegalArgumentException("No document start found");
        }
    }

    private static String cropToEnd(final String text) {

        final Matcher matcher = DOCUMENT_END_PATTERN.matcher(text);
        int lastMatchIndex = 0;

        while (matcher.find()) {

            lastMatchIndex = matcher.end();
        }

        return text.substring(0, lastMatchIndex);
    }

    private static String cleanUpBreaks(final String text) {

        return text.replaceAll("(\\s\n)+", "\n");
    }

    private static String joinSplitHeaders(final String text) {

        return text.replaceAll("([А-ЯІЄЇ, ]{30,})(\n)([А-ЯІЄЇ, ]{5,})", "$1 $3");
    }

    private static Map<String, Integer> getHeaders(final String text) {

        final Map<String, Integer> result = new LinkedHashMap<>();

        final Matcher matcher = HEADER_PATTERN.matcher(text);

        while (matcher.find()) {

            result.put(matcher.group().replace("\n", "").trim(), matcher.start());
        }

        return result;
    }

    private static Map<String, String> getChapters(final String text, final Map<String, Integer> headers) {

        final List<String> headerList = new ArrayList<>(headers.keySet());
        final List<Integer> headerIndexes = new ArrayList<>(headers.values());

        return IntStream.range(0, headerIndexes.size() - 1)
                .boxed()
                .map(chapterIndex -> Map.entry(
                        headerList.get(chapterIndex),
                        cleanUpChapterText(text.substring(headerIndexes.get(chapterIndex), headerIndexes.get(chapterIndex + 1)))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
    }

    private static String cleanUpChapterText(final String text) {

        return text.replace("\n", " ");
    }
}
