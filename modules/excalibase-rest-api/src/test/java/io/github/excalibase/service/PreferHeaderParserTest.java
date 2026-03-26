package io.github.excalibase.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PreferHeaderParser — all directives, edge cases, and combinations.
 */
class PreferHeaderParserTest {

    private PreferHeaderParser parser;

    @BeforeEach
    void setUp() {
        parser = new PreferHeaderParser();
    }

    // ==================== getReturn ====================

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    void getReturn_nullOrBlank_returnsRepresentation(String header) {
        assertThat(parser.getReturn(header)).isEqualTo(PreferHeaderParser.RETURN_REPRESENTATION);
    }

    @Test
    void getReturn_returnRepresentation_parsesCorrectly() {
        assertThat(parser.getReturn("return=representation"))
            .isEqualTo(PreferHeaderParser.RETURN_REPRESENTATION);
    }

    @Test
    void getReturn_returnHeadersOnly_parsesCorrectly() {
        assertThat(parser.getReturn("return=headers-only"))
            .isEqualTo(PreferHeaderParser.RETURN_HEADERS_ONLY);
    }

    @Test
    void getReturn_returnMinimal_parsesCorrectly() {
        assertThat(parser.getReturn("return=minimal"))
            .isEqualTo(PreferHeaderParser.RETURN_MINIMAL);
    }

    @Test
    void getReturn_noReturnDirective_returnsRepresentation() {
        assertThat(parser.getReturn("count=exact"))
            .isEqualTo(PreferHeaderParser.RETURN_REPRESENTATION);
    }

    @Test
    void getReturn_multipleDirectivesWithReturn_extractsReturn() {
        assertThat(parser.getReturn("count=exact, return=minimal"))
            .isEqualTo(PreferHeaderParser.RETURN_MINIMAL);
    }

    @Test
    void getReturn_multipleDirectivesReturnFirst_extractsCorrectly() {
        assertThat(parser.getReturn("return=headers-only, count=exact, resolution=merge-duplicates"))
            .isEqualTo(PreferHeaderParser.RETURN_HEADERS_ONLY);
    }

    @Test
    void getReturn_returnWithExtraSpaces_trimsCorrectly() {
        assertThat(parser.getReturn("  return=minimal  "))
            .isEqualTo(PreferHeaderParser.RETURN_MINIMAL);
    }

    // ==================== getCount ====================

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void getCount_nullOrBlank_returnsNull(String header) {
        assertThat(parser.getCount(header)).isNull();
    }

    @Test
    void getCount_countExact_returnsExact() {
        assertThat(parser.getCount("count=exact")).isEqualTo("exact");
    }

    @Test
    void getCount_countPlanned_returnsPlanned() {
        assertThat(parser.getCount("count=planned")).isEqualTo("planned");
    }

    @Test
    void getCount_countEstimated_returnsEstimated() {
        assertThat(parser.getCount("count=estimated")).isEqualTo("estimated");
    }

    @Test
    void getCount_noCountDirective_returnsNull() {
        assertThat(parser.getCount("return=representation")).isNull();
    }

    @Test
    void getCount_multipleDirectivesWithCount_extractsCount() {
        assertThat(parser.getCount("return=minimal, count=exact"))
            .isEqualTo("exact");
    }

    @Test
    void getCount_countWithExtraSpaces_trimsCorrectly() {
        assertThat(parser.getCount("  count=exact  ")).isEqualTo("exact");
    }

    // ==================== isUpsert ====================

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void isUpsert_nullOrBlank_returnsFalse(String header) {
        assertThat(parser.isUpsert(header)).isFalse();
    }

    @Test
    void isUpsert_mergeDuplicates_returnsTrue() {
        assertThat(parser.isUpsert("resolution=merge-duplicates")).isTrue();
    }

    @Test
    void isUpsert_noResolution_returnsFalse() {
        assertThat(parser.isUpsert("return=representation")).isFalse();
    }

    @Test
    void isUpsert_multipleDirectivesWithResolution_returnsTrue() {
        assertThat(parser.isUpsert("return=minimal, resolution=merge-duplicates, count=exact"))
            .isTrue();
    }

    @Test
    void isUpsert_otherResolutionValue_returnsFalse() {
        assertThat(parser.isUpsert("resolution=ignore-duplicates")).isFalse();
    }

    @Test
    void isUpsert_partialMatch_returnsFalse() {
        // "resolution=merge" does not equal "resolution=merge-duplicates"
        assertThat(parser.isUpsert("resolution=merge")).isFalse();
    }

    // ==================== isTxRollback ====================

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void isTxRollback_nullOrBlank_returnsFalse(String header) {
        assertThat(parser.isTxRollback(header)).isFalse();
    }

    @Test
    void isTxRollback_txRollback_returnsTrue() {
        assertThat(parser.isTxRollback("tx=rollback")).isTrue();
    }

    @Test
    void isTxRollback_noTx_returnsFalse() {
        assertThat(parser.isTxRollback("return=representation")).isFalse();
    }

    @Test
    void isTxRollback_txCommit_returnsFalse() {
        assertThat(parser.isTxRollback("tx=commit")).isFalse();
    }

    @Test
    void isTxRollback_multipleDirectivesWithTx_returnsTrue() {
        assertThat(parser.isTxRollback("return=minimal, tx=rollback"))
            .isTrue();
    }

    @Test
    void isTxRollback_partialMatch_returnsFalse() {
        assertThat(parser.isTxRollback("tx=roll")).isFalse();
    }

    // ==================== combined multi-directive scenarios ====================

    @Test
    void allDirectivesTogether_parsedCorrectly() {
        String header = "return=minimal, count=exact, resolution=merge-duplicates, tx=rollback";
        assertThat(parser.getReturn(header)).isEqualTo(PreferHeaderParser.RETURN_MINIMAL);
        assertThat(parser.getCount(header)).isEqualTo("exact");
        assertThat(parser.isUpsert(header)).isTrue();
        assertThat(parser.isTxRollback(header)).isTrue();
    }

    @Test
    void constants_haveExpectedValues() {
        assertThat(PreferHeaderParser.RETURN_REPRESENTATION).isEqualTo("representation");
        assertThat(PreferHeaderParser.RETURN_HEADERS_ONLY).isEqualTo("headers-only");
        assertThat(PreferHeaderParser.RETURN_MINIMAL).isEqualTo("minimal");
    }
}
