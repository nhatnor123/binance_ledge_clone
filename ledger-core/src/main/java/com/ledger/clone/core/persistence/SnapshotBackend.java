package com.ledger.clone.core.persistence;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Pluggable backend for snapshot storage.
 * <p>
 * Implementations handle where and how consistent state snapshots are stored
 * and retrieved. The default is {@link LocalSnapshotBackend} (local filesystem).
 * Future implementations may include S3, GCS, or other remote storage.
 */
public interface SnapshotBackend {

    /**
     * Uploads a snapshot directory to persistent storage.
     *
     * @param snapshotDir the local directory containing the snapshot data
     * @param snapshotId  unique identifier for this snapshot (e.g. "snapshot-12345")
     */
    void upload(Path snapshotDir, String snapshotId) throws IOException;

    /**
     * Downloads a snapshot from persistent storage to a local directory.
     *
     * @param snapshotId unique identifier of the snapshot to download
     * @param targetDir  local directory where the snapshot data will be written
     */
    void download(String snapshotId, Path targetDir) throws IOException;

    /**
     * Lists all available snapshots in descending order (newest first).
     *
     * @return list of snapshot IDs, empty if none exist
     */
    List<String> listSnapshots() throws IOException;
}
