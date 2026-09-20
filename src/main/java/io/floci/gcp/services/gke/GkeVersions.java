package io.floci.gcp.services.gke;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Orders GKE version strings numerically: {@code 1.9.0-gke.1} sorts before {@code 1.30.5-gke.1014001},
 * which a string comparison gets wrong. A version is {@code MAJOR.MINOR[.PATCH][-gke.N]}; a missing
 * patch or gke number counts as 0, so {@code 1.30} equals {@code 1.30.0-gke.0}. Anything outside
 * that shape (possible on {@code initialClusterVersion} and a node pool's explicit {@code version},
 * which are stored as sent) sorts after every well-formed version, among themselves by string, so
 * an unparseable value never becomes the minimum the cluster aggregate reports.
 */
final class GkeVersions {

    private static final Pattern VERSION = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-gke\\.(\\d+))?$");

    static final Comparator<String> ORDER = GkeVersions::compare;

    private GkeVersions() {
    }

    static int compare(String a, String b) {
        long[] left = parse(a);
        long[] right = parse(b);
        if (left == null || right == null) {
            if (left == null && right == null) {
                return a.compareTo(b);
            }
            return left == null ? 1 : -1;
        }
        for (int i = 0; i < left.length; i++) {
            int c = Long.compare(left[i], right[i]);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /** The lowest version in {@code versions}, ignoring nulls and blanks; empty when none remain. */
    static Optional<String> minimum(Collection<String> versions) {
        return versions.stream()
                .filter(v -> v != null && !v.isBlank())
                .min(ORDER);
    }

    private static long[] parse(String version) {
        Matcher m = VERSION.matcher(version);
        if (!m.matches()) {
            return null;
        }
        return new long[] {
                Long.parseLong(m.group(1)),
                Long.parseLong(m.group(2)),
                m.group(3) == null ? 0 : Long.parseLong(m.group(3)),
                m.group(4) == null ? 0 : Long.parseLong(m.group(4))};
    }
}
