package edu.kpi.stripper;

import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcludeHeaderFooterTextStripper extends PDFTextStripper {

    private static final float HEADER_BOUND = 0.067f;
    private static final float FOOTER_BOUND = 0.067f;

    private static final Map<Float, Float> HEADER_PIXEL_BOUNDS = new HashMap<>();
    private static final Map<Float, Float> FOOTER_PIXEL_BOUNDS = new HashMap<>();

    public ExcludeHeaderFooterTextStripper() throws IOException {
    }

    @Override
    protected void writeString(final String text, final List<TextPosition> textPositions) throws IOException {

        final TextPosition position = textPositions.get(0);

        final double headerBound = getHeaderPixelBound(position);
        final double footerBound = getFooterPixelBound(position);

        if (position.getY() > headerBound && position.getY() < footerBound) {

            super.writeString(text, textPositions);
        }
    }

    private float getHeaderPixelBound(final TextPosition position) {

        return HEADER_PIXEL_BOUNDS.computeIfAbsent(position.getPageHeight(), this::computeHeaderPixedBound);
    }

    private float computeHeaderPixedBound(final float pageHeight) {

        return pageHeight * HEADER_BOUND;
    }

    private float getFooterPixelBound(final TextPosition position) {

        return FOOTER_PIXEL_BOUNDS.computeIfAbsent(position.getPageHeight(), this::computeFooterPixedBound);
    }

    private float computeFooterPixedBound(final float pageHeight) {

        return pageHeight * (1 - FOOTER_BOUND);
    }
}
