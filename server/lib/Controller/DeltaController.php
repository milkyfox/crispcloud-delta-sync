<?php

declare(strict_types=1);

namespace OCA\CrispCloudDelta\Controller;

use OCA\CrispCloudDelta\Service\BlockMapService;
use OCA\CrispCloudDelta\Service\EtagMismatchException;
use OCP\AppFramework\Controller;
use OCP\AppFramework\Http;
use OCP\AppFramework\Http\JSONResponse;
use OCP\IRequest;
use OCP\IUserSession;

class DeltaController extends Controller {
    private const MAX_CHUNK_BYTES = 4 * 1024 * 1024; // 4 MB max per block/chunk

    private BlockMapService $blockMapService;
    private IUserSession $userSession;

    public function __construct(
        string $appName,
        IRequest $request,
        BlockMapService $blockMapService,
        IUserSession $userSession
    ) {
        parent::__construct($appName, $request);
        $this->blockMapService = $blockMapService;
        $this->userSession = $userSession;
    }

    private function getUserId(): ?string {
        $user = $this->userSession->getUser();
        return $user ? $user->getUID() : null;
    }

    /**
     * Resolve the target file path from, in priority order:
     *  1. X-File-Path header (rawurldecoded) — for WAF/web-server special-char safety
     *  2. {path} route argument (rawurldecoded)
     *  3. ?path= query parameter
     */
    private function resolvePath(string $path): string {
        $headerPath = $this->request->getHeader('X-File-Path');
        if ($headerPath !== null && trim((string)$headerPath) !== '') {
            return ltrim(rawurldecode(trim((string)$headerPath)), '/');
        }
        if ($path !== '') {
            return ltrim(rawurldecode($path), '/');
        }
        if (isset($_GET['path']) && trim((string)$_GET['path']) !== '') {
            return ltrim(trim((string)$_GET['path']), '/');
        }
        $paramPath = $this->request->getParam('path');
        if ($paramPath !== null && trim((string)$paramPath) !== '') {
            return ltrim(trim((string)$paramPath), '/');
        }
        return '';
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * GET /api/blockmap?path={path}&algo=fixed|fastcdc
     */
    public function getBlockMapQuery(): JSONResponse {
        return $this->getBlockMap('');
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/blocks?path={path}&offset=N&size=M
     * POST /api/blocks?path={path}&hash=HEX&size=M
     */
    public function putBlockQuery(): JSONResponse {
        return $this->putBlock('');
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/blocks?path={path}&offset=N&size=M
     * POST /api/blocks?path={path}&hash=HEX&size=M
     */
    public function putBlockQueryPut(): JSONResponse {
        return $this->putBlock('');
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * PUT /api/blocks/{path}?offset=N&size=M
     * PUT /api/blocks/{path}?hash=HEX&size=M
     */
    public function putBlockPut(string $path = ''): JSONResponse {
        return $this->putBlock($path);
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/finalize?path={path}&size=N
     */
    public function finalizeQuery(): JSONResponse {
        return $this->finalize('');
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/finalize?path={path}&size=N
     */
    public function finalizeQueryPut(): JSONResponse {
        return $this->finalize('');
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * PUT /api/finalize/{path}?size=N
     */
    public function finalizePut(string $path = ''): JSONResponse {
        return $this->finalize($path);
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * GET /api/blockmap/{path}?algo=fixed|fastcdc
     * GET /api/blockmap?path={path}&algo=fixed|fastcdc
     * Default algo is 'fixed' for 100% backward compatibility.
     */
    public function getBlockMap(string $path = ''): JSONResponse {
        $userId = $this->getUserId();
        if ($userId === null) {
            return new JSONResponse(['error' => 'Not authenticated'], Http::STATUS_UNAUTHORIZED);
        }

        $targetPath = $this->resolvePath($path);
        if ($targetPath === '') {
            return new JSONResponse(['error' => 'Path is required'], Http::STATUS_BAD_REQUEST);
        }

        $algo = $this->request->getParam('algo', 'fixed');
        $blockMap = $this->blockMapService->getBlockMap($userId, '/' . $targetPath, $algo);

        if ($blockMap === null) {
            return new JSONResponse(
                ['error' => 'File not found or not a regular file'],
                Http::STATUS_NOT_FOUND
            );
        }

        return new JSONResponse($blockMap);
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/blocks/{path}?offset=N&size=M  (Fixed chunking)
     * POST /api/blocks?path={path}&offset=N&size=M
     * POST /api/blocks/{path}?hash=HEX&size=M (FastCDC chunking)
     * POST /api/blocks?path={path}&hash=HEX&size=M
     */
    public function putBlock(string $path = ''): JSONResponse {
        $userId = $this->getUserId();
        if ($userId === null) {
            return new JSONResponse(['error' => 'Not authenticated'], Http::STATUS_UNAUTHORIZED);
        }

        $targetPath = $this->resolvePath($path);
        if ($targetPath === '') {
            return new JSONResponse(['error' => 'Path is required'], Http::STATUS_BAD_REQUEST);
        }

        // Reject oversized uploads before reading the body
        $contentLength = (int)($_SERVER['CONTENT_LENGTH'] ?? 0);
        if ($contentLength > self::MAX_CHUNK_BYTES) {
            return new JSONResponse(
                ['error' => 'Chunk size exceeds maximum of ' . self::MAX_CHUNK_BYTES . ' bytes'],
                413
            );
        }

        $hash = $this->request->getParam('hash');
        $offset = $this->request->getParam('offset');
        $size = (int)$this->request->getParam('size', '0');

        $data = file_get_contents('php://input');
        if ($data === false || strlen($data) === 0) {
            return new JSONResponse(['error' => 'Empty request body'], Http::STATUS_BAD_REQUEST);
        }

        if ($size > 0 && strlen($data) !== $size) {
            return new JSONResponse(
                ['error' => "Size mismatch: expected $size, got " . strlen($data)],
                Http::STATUS_BAD_REQUEST
            );
        }

        if ($size > self::MAX_CHUNK_BYTES || strlen($data) > self::MAX_CHUNK_BYTES) {
            return new JSONResponse(
                ['error' => 'Chunk size exceeds maximum of ' . self::MAX_CHUNK_BYTES . ' bytes'],
                413
            );
        }

        $identifier = $hash !== null ? (string)$hash : (string)((int)$offset);

        try {
            $this->blockMapService->writeBlock(
                $userId,
                '/' . $targetPath,
                $identifier,
                $data,
                $this->request->getHeader('If-Match')
            );
        } catch (EtagMismatchException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_PRECONDITION_FAILED);
        } catch (\InvalidArgumentException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_BAD_REQUEST);
        } catch (\OCP\Files\StorageNotAvailableException $e) {
            return new JSONResponse(['error' => $e->getMessage()], 507);
        } catch (\OCP\Files\ForbiddenException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_FORBIDDEN);
        } catch (\OCP\Files\NotFoundException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_NOT_FOUND);
        } catch (\Throwable $e) {
            error_log('crispcloud_delta: block write failed: ' . $e->getMessage());
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_INTERNAL_SERVER_ERROR);
        }

        return new JSONResponse(['status' => 'ok', 'identifier' => $identifier, 'size' => strlen($data)]);
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     *
     * POST /api/finalize/{path}?size=N
     * POST /api/finalize?path={path}&size=N
     * Optional JSON Body for FastCDC: {"recipe": ["hash1", "hash2", ...], "totalSize": N}
     */
    public function finalize(string $path = ''): JSONResponse {
        $userId = $this->getUserId();
        if ($userId === null) {
            return new JSONResponse(['error' => 'Not authenticated'], Http::STATUS_UNAUTHORIZED);
        }

        $targetPath = $this->resolvePath($path);
        if ($targetPath === '') {
            return new JSONResponse(['error' => 'Path is required'], Http::STATUS_BAD_REQUEST);
        }

        $sizeParam = $this->request->getParam('size');
        $newSize = ($sizeParam !== null) ? (int)$sizeParam : -1;
        $recipe = null;

        // 1. Try reading from Request parameters first (populated when Content-Type is application/json)
        $paramRecipe = $this->request->getParam('recipe');
        if (is_array($paramRecipe)) {
            $recipe = $paramRecipe;
        } else {
            $paramChunks = $this->request->getParam('chunks');
            if (is_array($paramChunks)) {
                $recipe = $paramChunks;
            }
        }
        $paramTotalSize = $this->request->getParam('totalSize');
        if ($paramTotalSize !== null) {
            $newSize = (int)$paramTotalSize;
        }

        // 2. If recipe not found via getParam, parse raw body (works for application/octet-stream, text/plain, etc.)
        if ($recipe === null) {
            $body = file_get_contents('php://input');
            if ($body !== false && strlen(trim($body)) > 0) {
                $json = json_decode($body, true);
                if (is_array($json)) {
                    if (isset($json['recipe']) && is_array($json['recipe'])) {
                        $recipe = $json['recipe'];
                    } elseif (isset($json['chunks']) && is_array($json['chunks'])) {
                        $recipe = $json['chunks'];
                    }
                    if (isset($json['totalSize'])) {
                        $newSize = (int)$json['totalSize'];
                    }
                }
            }
        }

        $mtimeHeader = $this->request->getHeader('X-OC-Mtime');
        $mtime = ($mtimeHeader !== null && is_numeric($mtimeHeader)) ? (int)$mtimeHeader : null;

        try {
            $res = $this->blockMapService->finalizeFile(
                $userId,
                '/' . $targetPath,
                $newSize,
                $recipe,
                $this->request->getHeader('If-Match'),
                $mtime
            );
            $response = new JSONResponse([
                'status' => 'finalized',
                'etag' => $res['etag'] ?? '',
                'fileId' => $res['fileId'] ?? '',
                'mtime' => $res['mtime'] ?? 0,
                'size' => $res['size'] ?? 0,
            ]);
            if (!empty($res['etag'])) {
                $response->addHeader('ETag', '"' . $res['etag'] . '"');
            }
            if (!empty($res['fileId'])) {
                $response->addHeader('OC-FileId', (string)$res['fileId']);
            }
            return $response;
        } catch (EtagMismatchException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_PRECONDITION_FAILED);
        } catch (\InvalidArgumentException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_BAD_REQUEST);
        } catch (\OCP\Files\StorageNotAvailableException $e) {
            return new JSONResponse(['error' => $e->getMessage()], 507);
        } catch (\OCP\Files\ForbiddenException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_FORBIDDEN);
        } catch (\OCP\Files\NotFoundException $e) {
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_NOT_FOUND);
        } catch (\Throwable $e) {
            error_log('crispcloud_delta: finalize failed: ' . $e->getMessage());
            return new JSONResponse(['error' => $e->getMessage()], Http::STATUS_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * @NoAdminRequired
     * @NoCSRFRequired
     * @PublicPage
     *
     * GET /api/status
     */
    public function status(): JSONResponse {
        return new JSONResponse([
            'app' => 'crispcloud_delta',
            'version' => '0.3.1',
            'status' => 'ok',
            'blockSize' => 4 * 1024 * 1024,
            'algorithm' => 'adler32+sha256',
            'supportedAlgorithms' => ['fastcdc', 'fixed'],
            'defaultAlgorithm' => 'fastcdc',
            'fastcdc' => [
                'minSize' => 262144,
                'avgSize' => 1048576,
                'maxSize' => 4194304,
            ],
        ]);
    }
}
