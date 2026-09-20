<?php

declare(strict_types=1);

namespace OCA\CrispCloudDelta\Service;

/**
 * FastCDC (Content-Defined Chunking) implementation in PHP.
 * Stream wrapper safe (no negative fseek, handles stream chunking).
 *
 * The algorithm contract matches the C++ client byte-for-byte:
 *  - Gear table seeded with 0x8a927c3d, LCG (1103515245, 12345) mod 2^31.
 *  - maskS = 0x00007fff (15 bits), maskL = 0x00003fff (14 bits).
 *  - Boundary scan between $min and $max bytes; cut when (fp & mask) === 0
 *    or the maximum chunk size is reached.
 */
class FastCdc {
    public const DEFAULT_MIN_SIZE = 262144;  // 256 KB
    public const DEFAULT_AVG_SIZE = 1048576; // 1 MB
    public const DEFAULT_MAX_SIZE = 4194304; // 4 MB

    /** @var array<int, int> */
    private static $gearTable = [];

    public static function initGearTable(): void {
        if (!empty(self::$gearTable)) {
            return;
        }
        $seed = 0x8a927c3d;
        for ($i = 0; $i < 256; $i++) {
            $seed = (int)(($seed * 1103515245 + 12345) & 0x7FFFFFFF);
            self::$gearTable[$i] = $seed;
        }
    }

    /**
     * Read exact number of bytes from stream unless EOF.
     *
     * @param resource $stream Open stream
     */
    private static function readExact($stream, int $length): string {
        $buf = '';
        while (strlen($buf) < $length && !feof($stream)) {
            $chunk = fread($stream, $length - strlen($buf));
            if ($chunk === false || strlen($chunk) === 0) {
                break;
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    /**
     * Chunk a stream using FastCDC.
     *
     * @param resource $stream Open stream
     * @param int $min Minimum chunk size
     * @param int $avg Target average chunk size
     * @param int $max Maximum chunk size
     * @return array<int, array{offset: int, size: int, hash: string}>
     */
    public static function chunkStream(
        $stream,
        int $min = self::DEFAULT_MIN_SIZE,
        int $avg = self::DEFAULT_AVG_SIZE,
        int $max = self::DEFAULT_MAX_SIZE
    ): array {
        self::initGearTable();
        $maskS = 0x00007fff; // 15 bits
        $maskL = 0x00003fff; // 14 bits

        $chunks = [];
        $offset = 0;
        $carryOver = '';

        while (!feof($stream) || strlen($carryOver) > 0) {
            $chunkData = $carryOver;
            $carryOver = '';

            // Ensure we have at least $min bytes
            if (strlen($chunkData) < $min) {
                $needed = $min - strlen($chunkData);
                $read = self::readExact($stream, $needed);
                $chunkData .= $read;
            }

            if (strlen($chunkData) === 0) {
                break;
            }

            // If less than $min bytes remain at EOF, this is the last chunk
            if (strlen($chunkData) <= $min && feof($stream)) {
                $size = strlen($chunkData);
                $chunks[] = [
                    'offset' => $offset,
                    'size' => $size,
                    'hash' => hash('sha256', $chunkData),
                ];
                $offset += $size;
                break;
            }

            // Scan between $min and $max for boundary
            $fp = 0;
            $cutPos = 0;
            $len = strlen($chunkData);

            // Read up to $max into buffer for scanning
            if ($len < $max && !feof($stream)) {
                $read = self::readExact($stream, $max - $len);
                $chunkData .= $read;
                $len = strlen($chunkData);
            }

            for ($j = $min; $j < $len; $j++) {
                $b = ord($chunkData[$j]);
                $fp = (($fp << 1) + self::$gearTable[$b]) & 0xFFFFFFFF;
                $mask = ($j < $avg) ? $maskS : $maskL;

                if (($fp & $mask) === 0 || ($j + 1) >= $max) {
                    $cutPos = $j + 1;
                    break;
                }
            }

            if ($cutPos === 0) {
                $cutPos = $len;
            }

            $currentChunk = substr($chunkData, 0, $cutPos);
            $carryOver = substr($chunkData, $cutPos);

            $size = strlen($currentChunk);
            $chunks[] = [
                'offset' => $offset,
                'size' => $size,
                'hash' => hash('sha256', $currentChunk),
            ];
            $offset += $size;
        }

        return $chunks;
    }
}
