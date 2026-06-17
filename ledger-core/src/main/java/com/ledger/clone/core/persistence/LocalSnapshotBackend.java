package com.ledger.clone.core.persistence;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;

@RequiredArgsConstructor
public class LocalSnapshotBackend implements SnapshotBackend {
    private final Path backupDir;

    @Override
    public void upload(Path snapshotDir, String snapshotId) throws IOException {
        Files.createDirectories(backupDir);
        var target = backupDir.resolve(snapshotId);
        deleteRecursively(target);
        try (var sources = Files.walk(snapshotDir)) {
            sources.forEach(source -> {
                try {
                    var dest = target.resolve(snapshotDir.relativize(source));
                    Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to copy: " + source, e);
                }
            });
        }
    }

    @Override
    public void download(String snapshotId, Path targetDir) throws IOException {
        var source = backupDir.resolve(snapshotId);
        if (!Files.exists(source)) {
            throw new IOException("Snapshot not found: " + snapshotId);
        }
        Files.createDirectories(targetDir);
        try (var sources = Files.walk(source)) {
            sources.forEach(src -> {
                try {
                    var dest = targetDir.resolve(source.relativize(src));
                    Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to restore: " + src, e);
                }
            });
        }
    }

    @Override
    public List<String> listSnapshots() throws IOException {
        if (!Files.exists(backupDir)) return List.of();
        try (var dirs = Files.list(backupDir)) {
            return dirs
                    .filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .sorted(Comparator.reverseOrder())
                    .toList();
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir).sorted(Comparator.reverseOrder())) {
            for (var p : (Iterable<Path>) walk::iterator) {
                Files.deleteIfExists(p);
            }
        }
    }
}
