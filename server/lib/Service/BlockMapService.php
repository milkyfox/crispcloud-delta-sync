<?php

declare(strict_types=1);

namespace OCA\CrispCloudDelta\Service;

use OCP\EventDispatcher\IEventDispatcher;
use OCP\Files\AppData\IAppDataFactory;
use OCP\Files\Events\Node\BeforeNodeWrittenEvent;
use OCP\Files\Events\Node\NodeWrittenEvent;
use OCP\Files\IAppData;
use OCP\Files\IRootFolder;
use OCP\Files\NotFoundException;
use OCP\ICacheFactory;
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
    /**
     * Whole-operation retries for lock errors that are not raised by the final
     * rename. Assembly itself takes no filesystem locks (see openReadStream()), so
     * this is only a safety net; renamePartWithRetry() waits out a busy target.
     */
    private const FINALIZE_LOCK_RETRIES = 3;
    private const FINALIZE_LOCK_RETRY_US = 200000;
    /**
     * How often the atomic rename of a finished assembly is re-attempted while the
     * target is locked. The part file is already complete at that point, so
     * retrying is cheap and never repeats the assembly.
     */
    private const COMMIT_LOCK_RETRIES = 60;
    private const COMMIT_LOCK_RETRY_US = 250000;

    /**
     * TTL of the app-level delta lock when a distributed cache is available.
     * Deliberately short: a worker killed mid-finalize must not keep a file
     * blocked for the instance-wide filelocking.ttl (1h by default).
     */
    private const LOCK_TTL_SECONDS = 120;
    /** How often a long-running assembly refreshes its delta lock. */
    private const LOCK_REFRESH_INTERVAL_SECONDS = 45;
    /** Staged chunks older than this are treated as abandoned and swept. */
    private const STAGING_STALE_AFTER_SECONDS = 86400;

    private IRootFolder $rootFolder;
    private IConfig $config;
    private ?IAppDataFactory $appDataFactory;
    private ?IAppData $appData = null;
    private ?ILockingProvider $lockingProvider = null;
    private ?ICacheFactory $cacheFactory;
    private ?IEventDispatcher $eventDispatcher;
    /** @var array{cache: \OCP\ICache, key: string, owner: string, refreshedAt: int}|null */
    private ?array $activeCacheLock = null;

    public function __construct(
        IRootFolder $rootFolder,
        IConfig $config,
        ?IAppDataFactory $appDataFactory = null,
        ?ILockingProvider $lockingProvider = null,
        ?ICacheFactory $cacheFactory = null,
        ?IEventDispatcher $eventDispatcher = null
    ) {
        $this->rootFolder = $rootFolder;
        $this->config = $config;
        $this->appDataFactory = $appDataFactory;
        $this->lockingProvider = $lockingProvider;
        $this->cacheFactory = $cacheFactory;
        $this->eventDispatcher = $eventDispatcher;
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
        // Opportunistic sweep of abandoned staging folders. Running it here (once
        // per block-map request) means crashed delta sessions are reclaimed without
        // relying on a cron job or on a probabilistic trigger.
        $this->cleanStaleStagingFolders();

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
        $handle = $this->openReadStream($file);
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

        $handle = $this->openReadStream($file);
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

            // Abandoned staging folders are swept deterministically by getBlockMap()
            // and finalizeFile(); no probabilistic trigger is needed on this hot path.
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
        // Reclaim staging folders left behind by crashed finalizes (see getBlockMap()).
        $this->cleanStaleStagingFolders();

        $fileInfoResult = [];
        for ($attempt = 0; ; $attempt++) {
            try {
                $this->withPathLock($userId, $path, function () use ($userId, $path, $newSize, $recipe, $ifMatch, $mtime, &$fileInfoResult): void {
                    $this->assertEtag($userId, $path, $ifMatch);
                    $userFolder = $this->getUserFolder($userId);
                    $file = $this->getOrCreateFile($userFolder, $path);

                    // Assemble directly into a sibling ".part" file inside the target's own
                    // folder (see openAssemblyTarget()). This keeps plaintext assembly data
                    // out of the system temp directory, avoids one full extra copy of the
                    // file, and turns crash leftovers into partial files that are encrypted
                    // under server-side encryption and invisible to clients.
                    $assembly = $this->openAssemblyTarget($userId, $path, $file, !empty($recipe));
                    $tempFilePath = $assembly['fallbackTempPath'];
                    $tempFile = $assembly['stream'];

                    $derivedSignatures = null;
                    try {
                        if (!empty($recipe)) {
                            // === CDC Recipe-Based Streaming Assembly ===
                            $derivedSignatures = $this->assembleWithRecipe($userId, $file, $path, $recipe, $tempFile);
                            $recomputeAlgo = 'fastcdc';
                            } else {
                                // === Legacy Offset-Based Patching ===
                                $derivedSignatures = $this->assembleWithOffsetBlocks($userId, $file, $path, $newSize, $tempFile);
                                $recomputeAlgo = 'fixed';
                            }

                        if ($newSize >= 0) {
                            $actualTempSize = ftell($tempFile);
                            if ($actualTempSize !== $newSize) {
                                throw new \RuntimeException("Assembled file size mismatch: expected $newSize bytes, actual $actualTempSize bytes");
                            }
                        }

                        // The source was read without a lock (see openReadStream()), so
                        // make sure a concurrent writer did not replace the target while
                        // we were assembling: if it did, fail the precondition instead of
                        // swapping in a result assembled from mixed content.
                        $startEtag = (string)$file->getEtag();
                        if ($startEtag !== '') {
                            try {
                                $currentEtag = (string)$userFolder->get($path)->getEtag();
                                if ($currentEtag !== $startEtag) {
                                    throw new EtagMismatchException($path, $startEtag, $currentEtag);
                                }
                            } catch (NotFoundException $e) {
                                // target disappeared while assembling; the rename recreates it
                            }
                        }

                        // Commit the assembled content atomically: write it into a sibling
                        // ".part" file and rename that over the target. Nextcloud's
                        // View::rename() treats a ".part" -> regular-file rename as a write
                        // (cache update, write hooks, versioning, file id preserved) and the
                        // underlying storage rename is atomic, so readers observe either the
                        // complete old file or the complete new file. A worker killed at any
                        // point can no longer leave the target truncated.
                        $this->commitAssembly($assembly, $userId, $path, $file, $tempFile);
                    } finally {
                        if (is_resource($tempFile)) {
                            fclose($tempFile);
                        }
                        if (is_string($tempFilePath)) {
                            @unlink($tempFilePath);
                        }
                    }

                    // Re-resolve the node: the atomic replace swapped the content behind
                    // the path, so touch the fresh node to apply the client's mtime.
                    $file = $userFolder->get($path);
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
                    if (is_array($derivedSignatures)) {
                        $derivedTotal = 0;
                        foreach ($derivedSignatures as $derivedSig) {
                            $derivedTotal += (int)$derivedSig['size'];
                        }
                        if ($derivedTotal === (int)$finalSize) {
                            $freshMap = [
                                'filePath' => $path,
                                'totalSize' => (int)$finalSize,
                                'algorithm' => $recomputeAlgo,
                                'blockCount' => count($derivedSignatures),
                                'signatures' => $derivedSignatures,
                                'createdAt' => (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
                            ];
                            if ($recomputeAlgo === 'fastcdc') {
                                $freshMap['minSize'] = FastCdc::DEFAULT_MIN_SIZE;
                                $freshMap['avgSize'] = FastCdc::DEFAULT_AVG_SIZE;
                                $freshMap['maxSize'] = FastCdc::DEFAULT_MAX_SIZE;
                            } else {
                                $freshMap['blockSize'] = self::BLOCK_SIZE;
                            }
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

        $rawSrcStream = $this->openReadStream($file);
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
                // Keep the delta lock alive across long assemblies.
                $this->refreshPathLock();

                $hash = is_array($item) ? ($item['hash'] ?? '') : (string)$item;
                $source = is_array($item) ? ($item['source'] ?? '') : '';

                if ($source === 'staged' || ($stageFolder && $this->nodeExists($stageFolder, $hash))) {
                    // Read from staged chunk file
                    if (!$stageFolder) {
                        throw new \RuntimeException("Staging folder not found for chunk [$hash]");
                    }
                    $stagedNode = $stageFolder->get($hash);
                    $stagedStream = $this->openReadStream($stagedNode);
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
     * Returns the fixed 4 MB block signatures derived from the assembled bytes, so
     * the caller never has to re-read the freshly committed file. Re-reading it in
     * the same request is not safe with server-side encryption: the sibling part
     * file has no cache entry, so the commit's cache update records the encrypted
     * size for the target and an immediate re-read fails signature verification
     * ("Bad Signature"). The plaintext assembly copy is read locally instead.
     *
     * @param resource $tempFile Open temp output stream
     * @return array<int, array{blockIndex: int, offset: int, size: int, weakHash: int, strongHash: string}>
     */
    private function assembleWithOffsetBlocks(string $userId, \OCP\Files\File $file, string $path, int $newSize, $tempFile): array {
        $srcStream = $this->openReadStream($file);
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
            // Keep the delta lock alive across long assemblies.
            $this->refreshPathLock();

            $stagedStream = $this->openReadStream($node);
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

        // The assembled content must cover the requested size; it is allowed to be
        // longer when the file is being shrunk, in which case the truncation below
        // drops the unchanged tail.
        if ($newSize >= 0 && $assembledLength < $newSize) {
            throw new \RuntimeException("Assembled file is shorter than the requested size: expected at least $newSize bytes, assembled $assembledLength bytes");
        }

        if ($newSize >= 0) {
            ftruncate($tempFile, $newSize);
            fseek($tempFile, $newSize, SEEK_SET);
        }

        return $this->buildFixedSignatures($tempFile, $newSize >= 0 ? $newSize : $assembledLength);
    }

    /**
     * Derive fixed 4 MB block signatures from an already assembled (plaintext)
     * stream, rewinding it first. Used so finalize never has to read back the
     * committed file inside the same request (see assembleWithOffsetBlocks()).
     *
     * @param resource $stream
     * @return array<int, array{blockIndex: int, offset: int, size: int, weakHash: int, strongHash: string}>
     */
    private function buildFixedSignatures($stream, int $size): array {
        $signatures = [];
        if ($size <= 0 || !is_resource($stream)) {
            return $signatures;
        }
        if (fseek($stream, 0, SEEK_SET) !== 0) {
            return $signatures;
        }

        $blockSize = self::BLOCK_SIZE;
        $blockCount = (int)ceil($size / $blockSize);
        for ($i = 0; $i < $blockCount; $i++) {
            $offset = $i * $blockSize;
            $remaining = min($blockSize, $size - $offset);
            $data = '';
            while (strlen($data) < $remaining) {
                $chunk = fread($stream, $remaining - strlen($data));
                if ($chunk === false || $chunk === '') {
                    break;
                }
                $data .= $chunk;
            }
            if ($data === '') {
                break;
            }
            $signatures[] = [
                'blockIndex' => $i,
                'offset' => $offset,
                'size' => strlen($data),
                'weakHash' => $this->adler32($data),
                'strongHash' => hash('sha256', $data),
            ];
        }

        return $signatures;
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
                if ($userStaging->getType() !== \OCP\Files\FileInfo::TYPE_FOLDER) {
                    continue;
                }
                foreach ($userStaging->getDirectoryListing() as $fileStaging) {
                    if ($fileStaging->getType() !== \OCP\Files\FileInfo::TYPE_FOLDER) {
                        continue;
                    }
                    if ($now - $fileStaging->getMTime() > self::STAGING_STALE_AFTER_SECONDS) {
                        try {
                            $fileStaging->delete();
                            error_log("crispcloud_delta: cleaned up stale staging folder " . $fileStaging->getName());
                        } catch (\Throwable $e) {}
                    }
                }
                // Drop empty per-user folders only once they are stale too: a freshly
                // created folder may belong to an upload that is just starting.
                if ($now - $userStaging->getMTime() > self::STAGING_STALE_AFTER_SECONDS
                    && empty($userStaging->getDirectoryListing())) {
                    try {
                        $userStaging->delete();
                    } catch (\Throwable $e) {}
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
        $lockKey = 'crispcloud_delta_' . hash('sha256', $userId . ':' . $path);

        // Preferred: a short-TTL distributed lock. Unlike ILockingProvider (whose TTL
        // is the instance-wide filelocking.ttl, 1h by default) a killed worker only
        // blocks this path until the key expires, i.e. LOCK_TTL_SECONDS.
        $lockCache = $this->getDistributedLockCache();
        if ($lockCache !== null) {
            $owner = bin2hex(random_bytes(16));
            $acquired = false;
            for ($attempt = 0; $attempt < 50; $attempt++) {
                if ($lockCache->add($lockKey, $owner, self::LOCK_TTL_SECONDS)) {
                    $acquired = true;
                    break;
                }
                usleep(100000);
            }
            if (!$acquired) {
                throw new \RuntimeException("Cannot lock delta path: $path");
            }

            $this->activeCacheLock = [
                'cache' => $lockCache,
                'key' => $lockKey,
                'owner' => $owner,
                'refreshedAt' => time(),
            ];
            try {
                $operation();
            } finally {
                $this->releaseCacheLock();
            }
            return;
        }

        if ($this->lockingProvider !== null) {
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

    /**
     * Distributed cache to use for the app-level delta lock, or null when this
     * instance has no distributed cache configured (a local cache cannot serialise
     * across PHP workers, so callers must fall back to ILockingProvider).
     */
    private function getDistributedLockCache(): ?\OCP\ICache {
        if ($this->cacheFactory === null) {
            return null;
        }
        $distributed = (string)$this->config->getSystemValue('memcache.distributed', '');
        if ($distributed === '') {
            return null;
        }
        try {
            return $this->cacheFactory->createDistributed('crispcloud_delta/locks');
        } catch (\Throwable $e) {
            return null;
        }
    }

    /**
     * Keep the app-level delta lock alive during long assemblies. Throttled to one
     * cache write per LOCK_REFRESH_INTERVAL_SECONDS, so it is safe to call from
     * per-chunk loops.
     */
    private function refreshPathLock(): void {
        if ($this->activeCacheLock === null) {
            return;
        }
        $now = time();
        if ($now - $this->activeCacheLock['refreshedAt'] < self::LOCK_REFRESH_INTERVAL_SECONDS) {
            return;
        }
        $this->activeCacheLock['refreshedAt'] = $now;
        try {
            $cache = $this->activeCacheLock['cache'];
            if ($cache->get($this->activeCacheLock['key']) === $this->activeCacheLock['owner']) {
                $cache->set($this->activeCacheLock['key'], $this->activeCacheLock['owner'], self::LOCK_TTL_SECONDS);
            }
        } catch (\Throwable $e) {}
    }

    /** Release the app-level delta lock if this process still owns it. */
    private function releaseCacheLock(): void {
        if ($this->activeCacheLock === null) {
            return;
        }
        $lock = $this->activeCacheLock;
        $this->activeCacheLock = null;
        try {
            if ($lock['cache']->get($lock['key']) === $lock['owner']) {
                $lock['cache']->remove($lock['key']);
            }
        } catch (\Throwable $e) {}
    }

    /**
     * Delete abandoned "<target>.<16 hex>.part" siblings of the file being finalized.
     *
     * An aborted finalize leaves its part file behind, and those files are invisible
     * to clients but still occupy disk, so each finalize reclaims its own leftovers.
     * Part files have no filecache entry by design, hence the raw storage directory is
     * inspected. Only files matching the exact part-file pattern of this target are
     * removed, and the caller holds the per-path delta lock, so a part file of an
     * in-flight finalize can never be hit.
     */
    private function cleanAbandonedPartFiles(\OCP\Files\File $file): void {
        try {
            $storage = $file->getStorage();
            $internalPath = $file->getInternalPath();
            $internalDir = dirname($internalPath);
            if ($internalDir === '.' || $internalDir === '/') {
                $internalDir = '';
            }

            $prefix = $file->getName() . '.';
            $prefixLen = strlen($prefix);

            $handle = $storage->opendir($internalDir === '' ? '/' : $internalDir);
            if (!is_resource($handle)) {
                return;
            }
            try {
                while (($entry = readdir($handle)) !== false) {
                    if (strpos($entry, $prefix) !== 0 || substr($entry, -5) !== '.part') {
                        continue;
                    }
                    $mid = substr($entry, $prefixLen, -5);
                    if (strlen($mid) !== 16 || !ctype_xdigit($mid)) {
                        continue;
                    }
                    $storage->unlink(($internalDir !== '' ? $internalDir . '/' : '') . $entry);
                    error_log('crispcloud_delta: removed abandoned part file ' . $entry);
                }
            } finally {
                closedir($handle);
            }
        } catch (\Throwable $e) {}
    }

    /**
     * Atomically replace the content of the file at $path with $sourceStream.
     *
     * Mirrors what Nextcloud's own WebDAV PUT does for ordinary uploads: stream the
     * bytes into a sibling "<name>.<random>.part" file, then rename that over the
     * target. View::rename() detects the partial-file source and performs a *write*
     * update (new size/etag, target file id kept), and the underlying storage rename
     * is atomic. A worker killed at any point therefore leaves the target holding its
     * previous complete content - never a half-written file.
     *
     * The View layer is used on purpose: a ".part" file has no filecache entry by
     * design, so the Node/File API refuses to write it ("update permission") and
     * View::shouldEmitHooks() short-circuits partial files so no app hooks fire.
     *
     * @param \OCP\Files\File $file Target file node
     * @param resource $sourceStream Rewound stream holding the new content
     */
    /**
     * Dispatch the typed node events that Nextcloud's own WebDAV uploads emit.
     *
     * The atomic replace writes a partial (".part") file and renames it over the
     * target. View::shouldEmitHooks() short-circuits partial files, so neither the
     * part-file write nor the rename triggers the write hooks that HookConnector
     * turns into Before/NodeWrittenEvent. Without these events files_versions would
     * never snapshot the previous content (and activity/author tracking would miss
     * the change), so dispatch them explicitly - exactly like the DAV layer does.
     */
    private function dispatchNodeWrittenEvents(\OCP\Files\File $file, bool $before): void {
        if ($this->eventDispatcher === null) {
            return;
        }
        try {
            $this->eventDispatcher->dispatchTyped($before
                ? new BeforeNodeWrittenEvent($file)
                : new NodeWrittenEvent($file));
        } catch (\Throwable $e) {
            error_log('crispcloud_delta: node write event dispatch failed: ' . $e->getMessage());
        }
    }

    /**
     * Open the output target for the assembled content.
     *
     * Preferred: a sibling "<name>.<random hex>.part" file inside the target's own
     * folder, written through the filesystem View. Compared with a system temp file
     * this (a) keeps plaintext assembly data out of the shared temp directory, so a
     * crashed worker cannot leave a plaintext copy of the file behind, (b) saves one
     * full read+write of the file (assembly -> part -> rename instead of
     * assembly -> temp -> part -> rename), and (c) makes crash leftovers ordinary
     * partial files inside the user's storage, which server-side encryption encrypts
     * and which Nextcloud's scanner/cache (and therefore every client) ignores.
     *
     * Falls back to the previous system-temp-file flow when the assembly is not a
     * plain sequential write (the legacy offset mode seeks and truncates its output)
     * or when the storage cannot hand out a seekable write stream.
     *
     * @param bool $sequential True when the caller only appends to the stream
     * @return array{view: ?\OC\Files\View, relPath: string, relPart: string, stream: resource, fallbackTempPath: ?string}
     */
    private function openAssemblyTarget(string $userId, string $path, \OCP\Files\File $file, bool $sequential): array {
        // Reclaim abandoned part files before creating a new one.
        $this->cleanAbandonedPartFiles($file);

        if ($sequential && class_exists('\OC\Files\View')) {
            $relPart = '';
            try {
                $view = new \OC\Files\View('/' . $userId . '/files');
                $relPath = ltrim($path, '/');
                $relDir = dirname($relPath);
                if ($relDir === '.' || $relDir === '/') {
                    $relDir = '';
                }
                $relPart = ($relDir !== '' ? $relDir . '/' : '')
                    . $file->getName() . '.' . bin2hex(random_bytes(8)) . '.part';

                $stream = $view->fopen($relPart, 'w');
                if (is_resource($stream)) {
                    $meta = stream_get_meta_data($stream);
                    if (!empty($meta['seekable'])) {
                        return [
                            'view' => $view,
                            'relPath' => $relPath,
                            'relPart' => $relPart,
                            'stream' => $stream,
                            'fallbackTempPath' => null,
                        ];
                    }
                    fclose($stream);
                    try {
                        $view->unlink($relPart);
                    } catch (\Throwable $e) {}
                }
            } catch (\Throwable $e) {
                error_log('crispcloud_delta: part-file assembly unavailable for ' . $path . ': ' . $e->getMessage());
            }
        }

        // Fallback: system temp file (previous behaviour).
        $tempFilePath = tempnam(sys_get_temp_dir(), 'nc_delta_');
        $stream = fopen($tempFilePath, 'w+b');
        if ($stream === false) {
            throw new \RuntimeException("Cannot create temp file for finalize");
        }
        return [
            'view' => null,
            'relPath' => '',
            'relPart' => '',
            'stream' => $stream,
            'fallbackTempPath' => $tempFilePath,
        ];
    }

    /**
     * Commit the assembled content: close the stream and atomically rename the part
     * file over the target. Falls back to the temp-file flow when the assembly ran
     * into a system temp file.
     */
    private function commitAssembly(array $assembly, string $userId, string $path, \OCP\Files\File $file, $stream): void {
        if ($assembly['view'] === null) {
            if (is_resource($stream)) {
                fseek($stream, 0, SEEK_SET);
            }
            $this->replaceFileAtomically($userId, $path, $file, $stream);
            return;
        }

        // Close first so buffered data (and encryption trailers) reach the part file
        // before it becomes the target.
        if (is_resource($stream)) {
            fclose($stream);
        }

        $view = $assembly['view'];
        $this->dispatchNodeWrittenEvents($file, true);
        if (!$this->renamePartWithRetry($view, $assembly['relPart'], $assembly['relPath'])) {
            try {
                $view->unlink($assembly['relPart']);
            } catch (\Throwable $e) {}
            throw new \RuntimeException("Cannot atomically replace $path");
        }
        $this->dispatchNodeWrittenEvents($file, false);
    }

    private function replaceFileAtomically(string $userId, string $path, \OCP\Files\File $file, $sourceStream): void {
        if (!class_exists('\OC\Files\View')) {
            // Legacy/unknown platform: keep the previous in-place write rather than
            // break finalize outright.
            error_log('crispcloud_delta: filesystem View unavailable; finalize is not atomic for ' . $path);
            if (is_resource($sourceStream)) {
                fseek($sourceStream, 0, SEEK_SET);
            }
            $file->putContent($sourceStream);
            return;
        }

        $view = new \OC\Files\View('/' . $userId . '/files');
        $relPath = ltrim($path, '/');
        $relDir = dirname($relPath);
        if ($relDir === '.' || $relDir === '/') {
            $relDir = '';
        }
        $relPart = ($relDir !== '' ? $relDir . '/' : '')
            . $file->getName() . '.' . bin2hex(random_bytes(8)) . '.part';

        $this->cleanAbandonedPartFiles($file);

        // Let files_versions/activity see the write before the old content disappears.
        $this->dispatchNodeWrittenEvents($file, true);

        if ($view->file_put_contents($relPart, $sourceStream) === false) {
            throw new \RuntimeException("Cannot write part file for $path");
        }

        if (!$this->renamePartWithRetry($view, $relPart, $relPath)) {
            try {
                $view->unlink($relPart);
            } catch (\Throwable $e) {}
            throw new \RuntimeException("Cannot atomically replace $path");
        }

        $this->dispatchNodeWrittenEvents($file, false);
    }

    /**
     * Open a binary read stream without holding a Nextcloud file lock.
     *
     * A View read stream keeps a shared lock for as long as it is open (View wraps
     * the stream and releases the lock from the stream's close callback), so a
     * worker killed while assembling or scanning a large file leaks that lock for
     * filelocking.ttl. Worse, Nextcloud extends a shared lock's TTL on every later
     * acquisition, so a leaked lock is kept alive by the very retries it blocks;
     * View::rename() then fails forever because upgrading the target lock from
     * shared to exclusive requires that no other shared holder exists.
     *
     * Reading through the storage layer keeps the encryption/quota wrappers but
     * takes no lock, so an aborted worker cannot block the file. The per-path delta
     * lock still serialises every delta operation for the same target.
     *
     * @param \OCP\Files\Node $node
     * @return resource|false
     */
    private function openReadStream($node) {
        try {
            if (method_exists($node, 'getStorage') && method_exists($node, 'getInternalPath')) {
                $storage = $node->getStorage();
                $internalPath = $node->getInternalPath();
                if ($storage !== null && is_string($internalPath) && $internalPath !== '') {
                    $stream = $storage->fopen($internalPath, 'rb');
                    if (is_resource($stream)) {
                        return $stream;
                    }
                }
            }
        } catch (\Throwable $e) {
            // fall back to the regular, locked view stream below
        }
        return $node->fopen('rb');
    }

    /**
     * Rename the assembled part file over the target, waiting out a temporarily
     * locked target. The part file is already complete at this point, so a retry
     * only repeats the rename and never the assembly.
     */
    private function renamePartWithRetry($view, string $relPart, string $relPath): bool {
        for ($attempt = 0; ; $attempt++) {
            try {
                return (bool)$view->rename($relPart, $relPath);
            } catch (\Throwable $e) {
                $isLocked = stripos($e->getMessage(), 'locked') !== false;
                if (!$isLocked || $attempt >= self::COMMIT_LOCK_RETRIES) {
                    throw $e;
                }
                usleep(self::COMMIT_LOCK_RETRY_US);
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
