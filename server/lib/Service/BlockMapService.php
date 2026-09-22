<?php

declare(strict_types=1);

namespace OCA\CrispCloudDelta\Service;

use OCP\Files\AppData\IAppDataFactory;
use OCP\Files\IAppData;
use OCP\Files\IRootFolder;
use OCP\Files\NotFoundException;
use OCP\IConfig;
use OCP\Lock\ILockingProvider;
use OCP\Lock\LockedException;

/**
 * Computes, caches, and serves block-level (Fixed 4MB) and FastCDC file indexes.
 * Uses isolated Nextcloud AppData storage to prevent staging and cache files
 * from leaking into user WebDAV sync directories.
 */
class BlockMapService {
    private const BLOCK_SIZE = 4 * 1024 * 1024; // 4 MB for fixed mode
    private const ADLER_MOD = 65521;
    private const FINALIZE_LOCK_RETRIES = 200;
    private const FINALIZE_LOCK_RETRY_US = 100000;

    private IRootFolder $rootFolder;
    private IConfig $config;
    private ?IAppDataFactory $appDataFactory;
    private ?IAppData $appData = null;
    private ?ILockingProvider $lockingProvider = null;

    public function __construct(
        IRootFolder $rootFolder,
        IConfig $config,
        ?IAppDataFactory $appDataFactory = null,
        ?ILockingProvider $lockingProvider = null
    ) {
        $this->rootFolder = $rootFolder;
        $this->config = $config;
        $this->appDataFactory = $appDataFactory;
        $this->lockingProvider = $lockingProvider;
    }

    private function getAppDataFolder(): \OCP\Files\Folder {
        if ($this->appData === null) {
            if ($this->appDataFactory !== null) {
                try {
                    $this->appData = $this->appDataFactory->get('crispcloud_delta');
                } catch (\Throwable $e) {}
            }
        }
        if ($this->appData instanceof \OCP\Files\Folder) {
            return $this->appData;
        }

        try {
            $instanceId = $this->config->getSystemValue('instanceid', 'nc');
            $appDataRoot = $this->rootFolder->get('appdata_' . $instanceId);
            return $this->getOrCreateFolder($appDataRoot, 'crispcloud_delta');
        } catch (\Throwable $e) {
            return $this->getOrCreateFolder($this->rootFolder, 'appdata_crispcloud_delta');
        }
    }

    private function getUserFolder(string $userId): \OCP\Files\Folder {
        if (class_exists('\OC\Files\Filesystem')) {
            try {
                \OC\Files\Filesystem::initMountPoints($userId);
            } catch (\Throwable $e) {}
        }
        $folder = $this->rootFolder->getUserFolder($userId);

        // Auto-cleanup legacy user-visible folders if present
        try {
            if ($folder->nodeExists('.crispcloud_delta')) {
                $folder->get('.crispcloud_delta')->delete();
            }
        } catch (\Throwable $e) {}
        try {
            if ($folder->nodeExists('.crispcloud_delta_staging')) {
                $folder->get('.crispcloud_delta_staging')->delete();
            }
        } catch (\Throwable $e) {}

        return $folder;
    }

    /**
     * Get or compute the block map for a file.
     *
     * @param string $userId User ID
     * @param string $path Relative path
     * @param string $algo 'fixed' (default) or 'fastcdc'
     */
    public function getBlockMap(string $userId, string $path, string $algo = 'fixed'): ?array {
        $userFolder = $this->getUserFolder($userId);

        try {
            $file = $userFolder->get($path);
        } catch (NotFoundException $e) {
            return null;
        }

        if ($file->getType() !== \OCP\Files\FileInfo::TYPE_FILE) {
            return null;
        }

        $etag = $file->getEtag();
        $fileSize = $file->getSize();

        // Check cache
        $cached = $this->loadCachedBlockMap($userId, $path, $algo);
        if ($cached !== null && isset($cached['etag']) && $cached['etag'] === $etag) {
            return $cached;
        }

        // Compute fresh block map
        error_log("crispcloud_delta: computing $algo block map for $path ($fileSize bytes)");

        if ($algo === 'fastcdc') {
            $blockMap = $this->computeFastCdcMap($file, $path);
        } else {
            $blockMap = $this->computeFixedBlockMap($file, $path);
        }
        $blockMap['etag'] = $etag;

        // Cache it
        $this->saveCachedBlockMap($userId, $path, $algo, $blockMap);

        return $blockMap;
    }

    /**
     * Compute FastCDC Content-Defined Chunk map.
     */
    private function computeFastCdcMap(\OCP\Files\File $file, string $path): array {
        $size = $file->getSize();
        $handle = $file->fopen('rb');
        if ($handle === false) {
            throw new \RuntimeException("Cannot open file: $path");
        }

        try {
            $rawChunks = FastCdc::chunkStream($handle);
        } finally {
            if (is_resource($handle)) {
                fclose($handle);
            }
        }

        $signatures = [];
        foreach ($rawChunks as $idx => $chunk) {
            $signatures[] = [
                'chunkIndex' => $idx,
                'offset' => $chunk['offset'],
                'size' => $chunk['size'],
                'hash' => $chunk['hash'],
                'strongHash' => $chunk['hash'],
            ];
        }

        return [
            'filePath' => $path,
            'totalSize' => $size,
            'algorithm' => 'fastcdc',
            'minSize' => FastCdc::DEFAULT_MIN_SIZE,
            'avgSize' => FastCdc::DEFAULT_AVG_SIZE,
            'maxSize' => FastCdc::DEFAULT_MAX_SIZE,
            'blockCount' => count($signatures),
            'signatures' => $signatures,
            'createdAt' => (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
        ];
    }

    /**
     * Compute legacy fixed 4MB block map.
     */
    private function computeFixedBlockMap(\OCP\Files\File $file, string $path): array {
        $size = $file->getSize();
        $blockSize = self::BLOCK_SIZE;
        $blockCount = $size === 0 ? 0 : (int)ceil($size / $blockSize);
        $signatures = [];

        $handle = $file->fopen('rb');
        if ($handle === false) {
            throw new \RuntimeException("Cannot open file: $path");
        }

        try {
            for ($i = 0; $i < $blockCount; $i++) {
                $offset = $i * $blockSize;
                $remaining = min($blockSize, $size - $offset);
                $data = '';
                while (strlen($data) < $remaining) {
                    $chunk = fread($handle, $remaining - strlen($data));
                    if ($chunk === false || $chunk === '') {
                        break;
                    }
                    $data .= $chunk;
                }
                if (strlen($data) === 0) {
                    break;
                }
                $actualSize = strlen($data);

                $signatures[] = [
                    'blockIndex' => $i,
                    'offset' => $offset,
                    'size' => $actualSize,
                    'weakHash' => $this->adler32($data),
                    'strongHash' => hash('sha256', $data),
                ];
            }
        } finally {
            if (is_resource($handle)) {
                fclose($handle);
            }
        }

        return [
            'filePath' => $path,
            'totalSize' => $size,
            'algorithm' => 'fixed',
            'blockSize' => $blockSize,
            'blockCount' => $blockCount,
            'signatures' => $signatures,
            'createdAt' => (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
        ];
    }

    private function adler32(string $data): int {
        $a = 1;
        $b = 0;
        $len = strlen($data);
        for ($i = 0; $i < $len; $i++) {
            $a = ($a + ord($data[$i])) % self::ADLER_MOD;
            $b = ($b + $a) % self::ADLER_MOD;
        }
        return ($b << 16) | $a;
    }

    /**
     * Write an uploaded block or FastCDC chunk into the isolated AppData staging folder.
     *
     * @param string $identifier Either a 64-char hex SHA-256 hash (FastCDC mode) or
     *                           a numeric offset (legacy fixed mode)
     */
    public function writeBlock(
        string $userId,
        string $path,
        string $identifier,
        string $data,
        ?string $ifMatch = null
    ): void {
        $this->withPathLock($userId, $path, function () use ($userId, $path, $identifier, $data, $ifMatch): void {
            $this->assertEtag($userId, $path, $ifMatch);
            $userFolder = $this->getUserFolder($userId);
            $file = $this->getOrCreateFile($userFolder, $path);
            if ($file->getType() !== \OCP\Files\FileInfo::TYPE_FILE) {
                throw new \RuntimeException("Not a file: $path");
            }

            $stageFolder = $this->getStageFolder($userId, $path);
            $name = trim((string)$identifier);
            if ($name === '') {
                throw new \InvalidArgumentException("Chunk identifier/hash is required");
            }

            if (!preg_match('/^[a-f0-9]{64}$/i', $name) && !preg_match('/^[0-9]+$/', $name)) {
                throw new \InvalidArgumentException("Invalid chunk identifier format: $name");
            }

            if (preg_match('/^[a-f0-9]{64}$/i', $name)) {
                $actualHash = hash('sha256', $data);
                if (strcasecmp($actualHash, $name) !== 0) {
                    throw new \InvalidArgumentException("Chunk hash integrity failure: expected $name, computed $actualHash");
                }
            }

            try {
                $stageFile = $stageFolder->get($name);
                $stageFile->putContent($data);
            } catch (NotFoundException $e) {
                $stageFile = $stageFolder->newFile($name);
                $stageFile->putContent($data);
            }
            error_log("crispcloud_delta: staged chunk [$name] (" . strlen($data) . " bytes) for $path in AppData");

            if (random_int(1, 50) === 1) {
                $this->cleanStaleStagingFolders();
            }
        });
    }

    /**
     * Finalize file assembly after delta uploads.
     *
     * @param int $newSize Expected final size, or -1 to skip the size check
     * @param array|null $recipe Ordered list of chunk hashes (FastCDC mode); each item
     *                           is either a string hash or ['hash' => ...]
     * @param string|null $ifMatch Optional ETag precondition
     * @param int|null $mtime Optional mtime in seconds (from X-OC-Mtime)
     * @return array{etag: string, fileId: string, mtime: int, size: int}
     */
    public function finalizeFile(
        string $userId,
        string $path,
        int $newSize = -1,
        ?array $recipe = null,
        ?string $ifMatch = null,
        ?int $mtime = null
    ): array {
        $fileInfoResult = [];
        for ($attempt = 0; ; $attempt++) {
            try {
                $this->withPathLock($userId, $path, function () use ($userId, $path, $newSize, $recipe, $ifMatch, $mtime, &$fileInfoResult): void {
                    $this->assertEtag($userId, $path, $ifMatch);
                    $userFolder = $this->getUserFolder($userId);
                    $file = $this->getOrCreateFile($userFolder, $path);

                    $tempFilePath = tempnam(sys_get_temp_dir(), 'nc_delta_');
                    $tempFile = fopen($tempFilePath, 'w+b');
                    if ($tempFile === false) {
                        throw new \RuntimeException("Cannot create temp file for finalize");
                    }

                    $derivedSignatures = null;
                    try {
                        if (!empty($recipe)) {
                            // === CDC Recipe-Based Streaming Assembly ===
                            $derivedSignatures = $this->assembleWithRecipe($userId, $file, $path, $recipe, $tempFile);
                            $recomputeAlgo = 'fastcdc';
                        } else {
                            // === Legacy Offset-Based Patching ===
                            $this->assembleWithOffsetBlocks($userId, $file, $path, $newSize, $tempFile);
                            $recomputeAlgo = 'fixed';
                        }

                        if ($newSize >= 0) {
                            $actualTempSize = ftell($tempFile);
                            if ($actualTempSize !== $newSize) {
                                throw new \RuntimeException("Assembled file size mismatch: expected $newSize bytes, actual $actualTempSize bytes");
                            }
                        }

                        // Stream assembled content into Nextcloud file node
                        fseek($tempFile, 0, SEEK_SET);
                        $file->putContent($tempFile);
                    } finally {
                        if (is_resource($tempFile)) {
                            fclose($tempFile);
                        }
                        @unlink($tempFilePath);
                    }

                    if ($mtime !== null && $mtime > 0) {
                        $file->touch($mtime);
                    } else {
                        $file->touch();
                    }

                    // Clean up isolated staging folder
                    $this->clearStagedBlocks($userId, $path);

                    // Reload fresh node from user folder to fetch updated post-touch cache metadata
                    try {
                        $reloadedFile = $userFolder->get($path);
                        $finalEtag = $reloadedFile->getEtag();
                        $finalFileId = (string)$reloadedFile->getId();
                        $finalMtime = $reloadedFile->getMTime();
                        $finalSize = $reloadedFile->getSize();
                    } catch (\Throwable $e) {
                        $reloadedFile = $file;
                        $finalEtag = $file->getEtag();
                        $finalFileId = (string)$file->getId();
                        $finalMtime = $file->getMTime();
                        $finalSize = $file->getSize();
                    }

                    // Build and cache the new blockmap in AppData using post-touch node & ETag.
                    // Preferred: derive it from the assembly recipe (no rescan — every chunk's
                    // hash/size/offset is known during assembly). Fall back to a full scan when
                    // no recipe is available or the derived data is inconsistent.
                    $freshMap = null;
                    if ($recomputeAlgo === 'fastcdc' && is_array($derivedSignatures)) {
                        $derivedTotal = 0;
                        foreach ($derivedSignatures as $derivedSig) {
                            $derivedTotal += (int)$derivedSig['size'];
                        }
                        if ($derivedTotal === (int)$finalSize) {
                            $freshMap = [
                                'filePath' => $path,
                                'totalSize' => (int)$finalSize,
                                'algorithm' => 'fastcdc',
                                'minSize' => FastCdc::DEFAULT_MIN_SIZE,
                                'avgSize' => FastCdc::DEFAULT_AVG_SIZE,
                                'maxSize' => FastCdc::DEFAULT_MAX_SIZE,
                                'blockCount' => count($derivedSignatures),
                                'signatures' => $derivedSignatures,
                                'createdAt' => (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
                            ];
                        }
                    }
                    if ($freshMap === null) {
                        $freshMap = ($recomputeAlgo === 'fastcdc')
                            ? $this->computeFastCdcMap($reloadedFile, $path)
                            : $this->computeFixedBlockMap($reloadedFile, $path);
                    }
                    $freshMap['etag'] = $finalEtag;
                    $this->saveCachedBlockMap($userId, $path, $recomputeAlgo, $freshMap);

                    $fileInfoResult = [
                        'etag' => $finalEtag,
                        'fileId' => $finalFileId,
                        'mtime' => $finalMtime,
                        'size' => $finalSize,
                    ];
                });
                return $fileInfoResult;
            } catch (EtagMismatchException $e) {
                throw $e;
            } catch (\Throwable $e) {
                $isLocked = stripos($e->getMessage(), 'locked') !== false;
                if (!$isLocked || $attempt >= self::FINALIZE_LOCK_RETRIES) {
                    throw $e;
                }
                usleep(self::FINALIZE_LOCK_RETRY_US);
            }
        }
    }

    /**
     * Assemble file using CDC Recipe (streaming, zero full-file RAM buffering).
     *
     * Returns the block map signatures for the assembled file, derived directly
     * from the recipe (every chunk's hash/size/offset is known during assembly),
     * so the caller can cache the new block map without a full re-scan.
     *
     * @param array $recipe Ordered list of chunk hashes; each item is either a
     *                      string hash or ['hash' => ...]
     * @param resource $tempFile Open temp output stream
     * @return array<int, array{chunkIndex: int, offset: int, size: int, hash: string, strongHash: string}>
     */
    private function assembleWithRecipe(string $userId, \OCP\Files\File $file, string $path, array $recipe, $tempFile): array {
        $stageFolder = null;
        try {
            $stageFolder = $this->getStageFolder($userId, $path);
        } catch (NotFoundException $e) {}

        // Load existing FastCDC blockmap to resolve existing chunk offsets (must match current ETag)
        $cachedMap = $this->loadCachedBlockMap($userId, $path, 'fastcdc');
        if ($cachedMap === null || !isset($cachedMap['signatures']) || !isset($cachedMap['etag']) || $cachedMap['etag'] !== $file->getEtag()) {
            $cachedMap = $this->computeFastCdcMap($file, $path);
        }
        $existingChunksByHash = [];
        if (isset($cachedMap['signatures'])) {
            foreach ($cachedMap['signatures'] as $sig) {
                $existingChunksByHash[$sig['hash']] = $sig;
            }
        }

        $rawSrcStream = $file->fopen('rb');
        if ($rawSrcStream === false) {
            throw new \RuntimeException("Cannot open source file for recipe assembly: $path");
        }
        $tempSeekableFile = null;
        $tempSeekablePath = null;
        $srcStream = false;

        $derivedSignatures = [];
        $derivedOffset = 0;
        $chunkIndex = 0;

        if (is_resource($rawSrcStream)) {
            $meta = stream_get_meta_data($rawSrcStream);
            if (!empty($meta['seekable'])) {
                $srcStream = $rawSrcStream;
            } else {
                // Buffer non-seekable stream (S3, Object Storage, SSE) to local seekable temp file
                $tempSeekablePath = tempnam(sys_get_temp_dir(), 'nc_delta_src_');
                $tempSeekableFile = fopen($tempSeekablePath, 'w+b');
                if ($tempSeekableFile !== false) {
                    stream_copy_to_stream($rawSrcStream, $tempSeekableFile);
                    fseek($tempSeekableFile, 0, SEEK_SET);
                    $srcStream = $tempSeekableFile;
                }
                fclose($rawSrcStream);
                $rawSrcStream = false;
            }
        }

        try {
            foreach ($recipe as $item) {
                $hash = is_array($item) ? ($item['hash'] ?? '') : (string)$item;
                $source = is_array($item) ? ($item['source'] ?? '') : '';

                if ($source === 'staged' || ($stageFolder && $this->nodeExists($stageFolder, $hash))) {
                    // Read from staged chunk file
                    if (!$stageFolder) {
                        throw new \RuntimeException("Staging folder not found for chunk [$hash]");
                    }
                    $stagedNode = $stageFolder->get($hash);
                    $stagedStream = $stagedNode->fopen('rb');
                    if ($stagedStream === false) {
                        throw new \RuntimeException("Cannot open staged chunk stream for [$hash]");
                    }
                    try {
                        $copied = stream_copy_to_stream($stagedStream, $tempFile);
                        if ($copied === false) {
                            throw new \RuntimeException("Failed to copy staged chunk [$hash] to destination");
                        }
                    } finally {
                        if (is_resource($stagedStream)) {
                            fclose($stagedStream);
                        }
                    }
                    $chunkSize = (int)$copied;
                } elseif ($source === 'existing' && isset($item['offset']) && isset($item['size']) && $srcStream !== false) {
                    // Explicit existing offset/size
                    fseek($srcStream, (int)$item['offset'], SEEK_SET);
                    $copied = stream_copy_to_stream($srcStream, $tempFile, (int)$item['size']);
                    if ($copied === false || $copied !== (int)$item['size']) {
                        throw new \RuntimeException("Failed to copy existing chunk from source file at offset {$item['offset']}");
                    }
                    $chunkSize = (int)$item['size'];
                } elseif (isset($existingChunksByHash[$hash]) && $srcStream !== false) {
                    // Matched from existing chunk hash
                    $sig = $existingChunksByHash[$hash];
                    fseek($srcStream, (int)$sig['offset'], SEEK_SET);
                    $copied = stream_copy_to_stream($srcStream, $tempFile, (int)$sig['size']);
                    if ($copied === false || $copied !== (int)$sig['size']) {
                        throw new \RuntimeException("Failed to copy existing chunk [$hash] from source file");
                    }
                    $chunkSize = (int)$sig['size'];
                } else {
                    throw new \RuntimeException("Recipe chunk [$hash] not found in staged or existing file");
                }

                $normalizedHash = strtolower((string)$hash);
                $derivedSignatures[] = [
                    'chunkIndex' => $chunkIndex++,
                    'offset' => $derivedOffset,
                    'size' => $chunkSize,
                    'hash' => $normalizedHash,
                    'strongHash' => $normalizedHash,
                ];
                $derivedOffset += $chunkSize;
            }
        } finally {
            if (is_resource($rawSrcStream)) {
                fclose($rawSrcStream);
            }
            if (is_resource($tempSeekableFile)) {
                fclose($tempSeekableFile);
            }
            if ($tempSeekablePath !== null) {
                @unlink($tempSeekablePath);
            }
        }

        return $derivedSignatures;
    }

    private function nodeExists($folder, string $name): bool {
        try {
            $folder->get($name);
            return true;
        } catch (NotFoundException $e) {
            return false;
        }
    }

    /**
     * Assemble file using legacy offset-based patching (streaming, zero RAM buffering).
     *
     * @param resource $tempFile Open temp output stream
     */
    private function assembleWithOffsetBlocks(string $userId, \OCP\Files\File $file, string $path, int $newSize, $tempFile): void {
        $srcStream = $file->fopen('rb');
        if ($srcStream === false) {
            throw new \RuntimeException("Cannot open source file for offset assembly: $path");
        }
        try {
            stream_copy_to_stream($srcStream, $tempFile);
        } finally {
            if (is_resource($srcStream)) {
                fclose($srcStream);
            }
        }
        $sourceLength = ftell($tempFile);

        $stageFolder = $this->getStageFolder($userId, $path);
        $listing = $stageFolder->getDirectoryListing();
        $nodesByOffset = [];
        foreach ($listing as $node) {
            if ($node->getType() !== \OCP\Files\FileInfo::TYPE_FILE) {
                continue;
            }
            $offset = filter_var($node->getName(), FILTER_VALIDATE_INT);
            if ($offset === false || $offset < 0) {
                continue;
            }
            $nodesByOffset[(int)$offset] = $node;
        }
        ksort($nodesByOffset, SORT_NUMERIC);

        $assembledLength = $sourceLength;
        foreach ($nodesByOffset as $offset => $node) {
            $stagedStream = $node->fopen('rb');
            if ($stagedStream === false) {
                throw new \RuntimeException("Cannot open staged block stream for offset $offset of $path");
            }
            try {
                fseek($tempFile, (int)$offset, SEEK_SET);
                stream_copy_to_stream($stagedStream, $tempFile);
            } finally {
                if (is_resource($stagedStream)) {
                    fclose($stagedStream);
                }
            }
            $assembledLength = max($assembledLength, (int)$offset + (int)$node->getSize());
        }

        if ($newSize >= 0 && $assembledLength !== $newSize) {
            throw new \RuntimeException("Assembled file size mismatch: expected $newSize bytes, actual $assembledLength bytes");
        }

        if ($newSize >= 0) {
            ftruncate($tempFile, $newSize);
            fseek($tempFile, $newSize, SEEK_SET);
        }
    }

    public function assertEtag(string $userId, string $path, ?string $ifMatch): void {
        if ($ifMatch === null || trim($ifMatch) === '') {
            return;
        }

        $userFolder = $this->getUserFolder($userId);
        try {
            $file = $userFolder->get($path);
            $actual = $file->getEtag();
        } catch (NotFoundException $e) {
            throw new EtagMismatchException($path, $ifMatch, '(not found)');
        }

        $actualClean = trim(preg_replace('/^W\\//i', '', (string)$actual) ?? (string)$actual, '"');
        $expectedValues = array_map('trim', explode(',', $ifMatch));

        foreach ($expectedValues as $expected) {
            if ($expected === '*') {
                return;
            }
            $expectedClean = trim(preg_replace('/^W\\//i', '', $expected) ?? $expected, '"');
            if ($expectedClean === $actualClean) {
                return;
            }
        }

        throw new EtagMismatchException($path, $ifMatch, $actualClean);
    }

    private function getStageFolder(string $userId, string $path): \OCP\Files\Folder {
        $appData = $this->getAppDataFolder();
        $sub = 'staging/' . $userId . '/' . hash('sha256', $path);
        return $this->getOrCreateFolder($appData, $sub);
    }

    private function clearStagedBlocks(string $userId, string $path): void {
        try {
            $folder = $this->getStageFolder($userId, $path);
            $folder->delete();
        } catch (\Throwable $e) {}
    }

    private function cleanStaleStagingFolders(): void {
        try {
            $appData = $this->getAppDataFolder();
            $stagingRoot = $this->getOrCreateFolder($appData, 'staging');
            $now = time();
            foreach ($stagingRoot->getDirectoryListing() as $userStaging) {
                if ($userStaging->getType() === \OCP\Files\FileInfo::TYPE_FOLDER) {
                    foreach ($userStaging->getDirectoryListing() as $fileStaging) {
                        if ($fileStaging->getType() === \OCP\Files\FileInfo::TYPE_FOLDER) {
                            if ($now - $fileStaging->getMTime() > 86400) {
                                try {
                                    $fileStaging->delete();
                                    error_log("crispcloud_delta: cleaned up stale staging folder " . $fileStaging->getName());
                                } catch (\Throwable $e) {}
                            }
                        }
                    }
                    if (empty($userStaging->getDirectoryListing())) {
                        try {
                            $userStaging->delete();
                        } catch (\Throwable $e) {}
                    }
                }
            }
        } catch (\Throwable $e) {}
    }

    private function normalizePath(string $path): string {
        $path = str_replace('\\', '/', $path);
        return trim($path, '/');
    }

    private function getOrCreateFile($folder, string $path) {
        $path = $this->normalizePath($path);
        $parts = explode('/', $path);
        $fileName = array_pop($parts);
        $targetFolder = empty($parts) ? $folder : $this->getOrCreateFolder($folder, implode('/', $parts));
        try {
            return $targetFolder->get($fileName);
        } catch (NotFoundException $e) {
            return $targetFolder->newFile($fileName);
        }
    }

    private function getOrCreateFolder($folder, string $path) {
        $current = $folder;
        $path = $this->normalizePath($path);
        foreach (explode('/', $path) as $part) {
            if ($part === '') {
                continue;
            }
            try {
                $current = $current->get($part);
            } catch (NotFoundException $e) {
                $current = $current->newFolder($part);
            }
        }
        return $current;
    }

    private function withPathLock(string $userId, string $path, callable $operation): void {
        if ($this->lockingProvider !== null) {
            $lockKey = 'crispcloud_delta_' . hash('sha256', $userId . ':' . $path);
            $acquired = false;
            for ($attempt = 0; $attempt < 50; $attempt++) {
                try {
                    $this->lockingProvider->acquireLock($lockKey, ILockingProvider::LOCK_EXCLUSIVE);
                    $acquired = true;
                    break;
                } catch (LockedException $e) {
                    usleep(100000);
                }
            }
            if (!$acquired) {
                throw new \RuntimeException("Cannot lock delta path: $path");
            }
            try {
                $operation();
            } finally {
                try {
                    $this->lockingProvider->releaseLock($lockKey, ILockingProvider::LOCK_EXCLUSIVE);
                } catch (\Throwable $e) {}
            }
            return;
        }

        $lockPath = sys_get_temp_dir() . '/crispcloud_delta_' . hash('sha256', $userId . ':' . $path) . '.lock';
        $lock = fopen($lockPath, 'c');
        if ($lock === false) {
            throw new \RuntimeException("Cannot open delta lock for $path");
        }
        try {
            if (!flock($lock, LOCK_EX)) {
                throw new \RuntimeException("Cannot lock delta path: $path");
            }
            $operation();
        } finally {
            if (is_resource($lock)) {
                flock($lock, LOCK_UN);
                fclose($lock);
            }
        }
    }

    private function loadCachedBlockMap(string $userId, string $path, string $algo = 'fixed'): ?array {
        try {
            $appData = $this->getAppDataFolder();
            $cacheFolder = $this->getOrCreateFolder($appData, 'cache/' . $userId);
            $cacheName = hash('sha256', $path . ':' . $algo) . '.json';
            $cacheFile = $cacheFolder->get($cacheName);
            $json = $cacheFile->getContent();
            return json_decode($json, true);
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function saveCachedBlockMap(string $userId, string $path, string $algo, array $blockMap): void {
        try {
            $appData = $this->getAppDataFolder();
            $cacheFolder = $this->getOrCreateFolder($appData, 'cache/' . $userId);
            $cacheName = hash('sha256', $path . ':' . $algo) . '.json';
            $json = json_encode($blockMap, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            try {
                $cacheFile = $cacheFolder->get($cacheName);
                $cacheFile->putContent($json);
            } catch (NotFoundException $e) {
                $cacheFile = $cacheFolder->newFile($cacheName);
                $cacheFile->putContent($json);
            }
        } catch (\Throwable $e) {}
    }
}
