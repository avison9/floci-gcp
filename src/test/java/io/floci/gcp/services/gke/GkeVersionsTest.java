package io.floci.gcp.services.gke;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GkeVersionsTest {

    @Test
    void ordersNumericallyNotLexically() {
        // "1.9" sorts after "1.30" as a string; as a version it is older.
        assertTrue(GkeVersions.compare("1.9.0-gke.1", "1.30.5-gke.1014001") < 0);
        assertTrue(GkeVersions.compare("1.30.5-gke.1014001", "1.30.5-gke.999") > 0);
        assertTrue(GkeVersions.compare("1.30.5-gke.1", "1.30.10-gke.1") < 0);
        assertTrue(GkeVersions.compare("2.0.0-gke.1", "1.99.99-gke.99") > 0);
        assertEquals(0, GkeVersions.compare("1.30.5-gke.1014001", "1.30.5-gke.1014001"));
    }

    @Test
    void missingPatchAndGkeNumberCountAsZero() {
        assertEquals(0, GkeVersions.compare("1.30", "1.30.0-gke.0"));
        assertTrue(GkeVersions.compare("1.30", "1.30.0-gke.1") < 0);
        assertTrue(GkeVersions.compare("1.30.5", "1.30.5-gke.1") < 0);
    }

    @Test
    void unparseableVersionsSortLastAndNeverWinTheMinimum() {
        assertTrue(GkeVersions.compare("banana", "1.30.5-gke.1") > 0);
        // A component beyond the long range is unparseable, not an exception out of the comparison.
        String huge = "1.999999999999999999999.0-gke.1";
        assertTrue(GkeVersions.compare(huge, "1.30.5-gke.1") > 0);
        assertEquals(Optional.of("1.30.5-gke.1"), GkeVersions.minimum(List.of(huge, "1.30.5-gke.1")));
        assertTrue(GkeVersions.compare("apple", "banana") < 0);
        List<String> versions = Arrays.asList("banana", "1.30.5-gke.1014001", null, " ", "1.29.0-gke.1", "1.9.0-gke.1");
        assertEquals(Optional.of("1.9.0-gke.1"), GkeVersions.minimum(versions));
        assertEquals(Optional.empty(), GkeVersions.minimum(Arrays.asList(null, "")));
        assertEquals(Optional.of("banana"), GkeVersions.minimum(List.of("banana")));
    }
}
