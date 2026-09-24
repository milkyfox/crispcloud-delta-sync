#!/usr/bin/env python3
"""
Live integration test for the crispcloud_delta server app.

Tests:
  1. /api/status  — app is reachable and returns correct JSON
  2. Block map accuracy — computed hashes match local file
  3. Delta update (file modification) — upload only changed block, verify bit-perfect result
  4. File shrink — finalize with ?size= param, verify server truncates correctly
  5. File grow — new block added at the end, verify server extends correctly
  6. Stale ETag — concurrent mutation is rejected without overwriting newer data
  7. Staged visibility — readers see the old file until finalize commits it
  8. In-flight reader stress — concurrent GETs never observe a mixed file

Usage:
  python3 test_live.py http://168.119.190.252:8888 admin Nextcloud2026!
"""

import sys
import os
import hashlib
import struct
import json
import tempfile
import urllib.request
import urllib.parse
import urllib.error
import base64
import threading

BLOCK_SIZE = 4 * 1024 * 1024  # 4 MB, must match server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def adler32(data: bytes) -> int:
    MOD = 65521
    a, b = 1, 0
    for byte in data:
        a = (a + byte) % MOD
        b = (b + a) % MOD
    return (b << 16) | a


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def auth_header(user: str, password: str) -> str:
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {creds}"


def request(method: str, url: str, user: str, password: str,
            body: bytes = None, content_type: str = None,
            extra_headers: dict = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", auth_header(user, password))
    req.add_header("OCS-APIREQUEST", "true")
    if content_type:
        req.add_header("Content-Type", content_type)
    if extra_headers:
        for k, v in extra_headers.items():
            req.add_header(k, v)
    if body is not None:
        req.data = body
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def webdav_put(base_url: str, user: str, password: str,
               remote_path: str, data: bytes) -> int:
    url = f"{base_url}/remote.php/dav/files/{user}/{remote_path.lstrip('/')}"
    code, _ = request("PUT", url, user, password, body=data,
                      content_type="application/octet-stream")
    return code


def webdav_get(base_url: str, user: str, password: str,
               remote_path: str) -> tuple[int, bytes]:
    url = f"{base_url}/remote.php/dav/files/{user}/{remote_path.lstrip('/')}"
    return request("GET", url, user, password)


def webdav_delete(base_url: str, user: str, password: str,
                  remote_path: str) -> int:
    url = f"{base_url}/remote.php/dav/files/{user}/{remote_path.lstrip('/')}"
    code, _ = request("DELETE", url, user, password)
    return code


def delta_status(base_url: str, user: str, password: str) -> dict:
    code, body = request("GET", f"{base_url}/index.php/apps/crispcloud_delta/api/status",
                         user, password)
    assert code == 200, f"status returned HTTP {code}"
    return json.loads(body)


def delta_blockmap(base_url: str, user: str, password: str,
                   remote_path: str) -> tuple[int, dict]:
    url = f"{base_url}/index.php/apps/crispcloud_delta/api/blockmap/{remote_path.lstrip('/')}"
    code, body = request("GET", url, user, password)
    if code != 200:
        return code, {}
    return code, json.loads(body)


def delta_put_block(base_url: str, user: str, password: str,
                    remote_path: str, offset: int, data: bytes,
                    if_match: str = None) -> int:
    qs = urllib.parse.urlencode({"offset": offset, "size": len(data)})
    url = (f"{base_url}/index.php/apps/crispcloud_delta/api/blocks"
           f"/{remote_path.lstrip('/')}?{qs}")
    headers = {"If-Match": f'"{if_match}"'} if if_match else None
    code, _ = request("POST", url, user, password, body=data,
                      content_type="application/octet-stream",
                      extra_headers=headers)
    return code


def delta_finalize(base_url: str, user: str, password: str,
                   remote_path: str, new_size: int,
                   if_match: str = None) -> int:
    qs = urllib.parse.urlencode({"size": new_size})
    url = (f"{base_url}/index.php/apps/crispcloud_delta/api/finalize"
           f"/{remote_path.lstrip('/')}?{qs}")
    headers = {"If-Match": f'"{if_match}"'} if if_match else None
    code, _ = request("POST", url, user, password, body=b"",
                      extra_headers=headers)
    return code


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results = []


def ok(name: str):
    print(f"  {PASS}  {name}")
    results.append((name, True))


def fail(name: str, reason: str):
    print(f"  {FAIL}  {name}: {reason}")
    results.append((name, False))


def test_status(base_url, user, password):
    print("\n[1] Status endpoint")
    try:
        s = delta_status(base_url, user, password)
        assert s.get("app") == "crispcloud_delta", f"unexpected app name: {s}"
        assert s.get("status") == "ok"
        assert s.get("blockSize") == BLOCK_SIZE
        ok("status returns correct JSON")
    except Exception as e:
        fail("status returns correct JSON", str(e))


def test_blockmap_accuracy(base_url, user, password):
    print("\n[2] Block map accuracy")
    remote_path = "_delta_test_blockmap.bin"

    # 10 MB file with known pattern
    content = bytes(range(256)) * (10 * 1024 * 1024 // 256)
    try:
        code = webdav_put(base_url, user, password, remote_path, content)
        assert code in (200, 201, 204), f"WebDAV PUT returned {code}"

        code, bm = delta_blockmap(base_url, user, password, remote_path)
        assert code == 200, f"blockmap returned {code}"

        # Verify block count
        expected_blocks = -(-len(content) // BLOCK_SIZE)  # ceil
        assert bm["blockCount"] == expected_blocks, \
            f"blockCount {bm['blockCount']} != {expected_blocks}"
        ok("block count is correct")

        # Verify each block's hashes
        for sig in bm["signatures"]:
            idx = sig["blockIndex"]
            offset = idx * BLOCK_SIZE
            block = content[offset: offset + BLOCK_SIZE]
            expected_weak = adler32(block)
            expected_strong = sha256(block)
            assert sig["weakHash"] == expected_weak, \
                f"block {idx} weak hash mismatch: {sig['weakHash']} != {expected_weak}"
            assert sig["strongHash"] == expected_strong, \
                f"block {idx} strong hash mismatch"
        ok("all block hashes match")

    except Exception as e:
        fail("block map accuracy", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_delta_update(base_url, user, password):
    print("\n[3] Delta update (modify middle of file)")
    remote_path = "_delta_test_update.bin"

    # 12 MB file: 3 × 4 MB blocks
    original = bytearray(12 * 1024 * 1024)
    for i in range(len(original)):
        original[i] = i % 251

    modified = bytearray(original)
    # Change 8 bytes in the middle of block 1 (offset 4MB + 16)
    for i in range(8):
        modified[BLOCK_SIZE + 16 + i] ^= 0xFF

    try:
        code = webdav_put(base_url, user, password, remote_path, bytes(original))
        assert code in (200, 201, 204), f"initial PUT returned {code}"

        # Upload only block 1 (the changed one)
        changed_block = bytes(modified[BLOCK_SIZE: 2 * BLOCK_SIZE])
        code = delta_put_block(base_url, user, password, remote_path,
                               offset=BLOCK_SIZE, data=changed_block)
        assert code == 200, f"put_block returned {code}"
        ok("block upload accepted")

        code = delta_finalize(base_url, user, password, remote_path,
                              new_size=len(modified))
        assert code == 200, f"finalize returned {code}"
        ok("finalize accepted")

        # Download and verify
        code, server_content = webdav_get(base_url, user, password, remote_path)
        assert code == 200, f"WebDAV GET returned {code}"
        assert len(server_content) == len(modified), \
            f"size mismatch: {len(server_content)} != {len(modified)}"
        if server_content != bytes(modified):
            first = next((i for i, (actual, expected) in enumerate(
                zip(server_content, modified)) if actual != expected), None)
            raise AssertionError(
                f"content mismatch after delta update at offset {first}: "
                f"got={server_content[first] if first is not None else None} "
                f"expected={modified[first] if first is not None else None} "
                f"sha256={hashlib.sha256(server_content).hexdigest()}"
            )
        ok("server file is bit-perfect after delta update")

    except Exception as e:
        fail("delta update", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_file_shrink(base_url, user, password):
    print("\n[4] File shrink (finalize must truncate)")
    remote_path = "_delta_test_shrink.bin"

    # 12 MB → shrink to 10 MB (drop last 2 MB)
    original = bytes(b'\xAB' * (12 * 1024 * 1024))
    shrunk = bytes(b'\xAB' * (10 * 1024 * 1024))

    try:
        code = webdav_put(base_url, user, password, remote_path, original)
        assert code in (200, 201, 204)

        # No blocks to upload (all remaining blocks are identical), just finalize
        code = delta_finalize(base_url, user, password, remote_path,
                              new_size=len(shrunk))
        assert code == 200, f"finalize returned {code}"

        code, server_content = webdav_get(base_url, user, password, remote_path)
        assert code == 200
        assert len(server_content) == len(shrunk), \
            f"server size {len(server_content)} != expected {len(shrunk)} — truncation failed"
        assert server_content == shrunk, "content mismatch after shrink"
        ok("server file correctly truncated after shrink")

    except Exception as e:
        fail("file shrink", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_file_grow(base_url, user, password):
    print("\n[5] File grow (new blocks at the end)")
    remote_path = "_delta_test_grow.bin"

    # 8 MB → grow to 12 MB
    original = bytes(b'\xCD' * (8 * 1024 * 1024))
    grown = bytes(b'\xCD' * (8 * 1024 * 1024)) + bytes(b'\xEF' * (4 * 1024 * 1024))

    try:
        code = webdav_put(base_url, user, password, remote_path, original)
        assert code in (200, 201, 204)

        # Upload the new block (block 2, offset 8 MB)
        new_block = grown[2 * BLOCK_SIZE: 3 * BLOCK_SIZE]
        code = delta_put_block(base_url, user, password, remote_path,
                               offset=2 * BLOCK_SIZE, data=new_block)
        assert code == 200, f"put_block for new block returned {code}"

        code = delta_finalize(base_url, user, password, remote_path,
                              new_size=len(grown))
        assert code == 200

        code, server_content = webdav_get(base_url, user, password, remote_path)
        assert code == 200
        assert len(server_content) == len(grown), \
            f"server size {len(server_content)} != expected {len(grown)}"
        if server_content != grown:
            first = next((i for i, (actual, expected) in enumerate(
                zip(server_content, grown)) if actual != expected), None)
            raise AssertionError(
                f"content mismatch after grow at offset {first}: "
                f"got={server_content[first] if first is not None else None} "
                f"expected={grown[first] if first is not None else None} "
                f"sha256={hashlib.sha256(server_content).hexdigest()}"
            )
        ok("server file correctly extended after grow")

    except Exception as e:
        fail("file grow", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_stale_etag_rejected(base_url, user, password):
    print("\n[6] Stale ETag mutation is rejected")
    remote_path = "_delta_test_stale_etag.bin"
    original = bytes(b'\x11' * BLOCK_SIZE)
    concurrent = bytes(b'\x22' * BLOCK_SIZE)

    try:
        code = webdav_put(base_url, user, password, remote_path, original)
        assert code in (200, 201, 204)
        code, blockmap = delta_blockmap(base_url, user, password, remote_path)
        assert code == 200 and blockmap.get("etag"), "missing initial ETag"
        stale_etag = blockmap["etag"]

        code = webdav_put(base_url, user, password, remote_path, concurrent)
        assert code in (200, 201, 204)

        code = delta_put_block(
            base_url, user, password, remote_path, 0, original,
            if_match=stale_etag)
        assert code == 412, f"stale block write returned {code}, expected 412"
        code = delta_finalize(
            base_url, user, password, remote_path, len(original),
            if_match=stale_etag)
        assert code == 412, f"stale finalize returned {code}, expected 412"
        ok("stale block and finalize requests return HTTP 412")
    except Exception as e:
        fail("stale ETag rejection", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_staged_reader_visibility(base_url, user, password):
    print("\n[7] Staged finalize is invisible until commit")
    remote_path = "_delta_test_staged_visibility.bin"
    original = bytes([0x31]) * BLOCK_SIZE
    changed = bytes([0xE2]) * BLOCK_SIZE

    try:
        code = webdav_put(base_url, user, password, remote_path, original)
        assert code in (200, 201, 204)
        code, blockmap = delta_blockmap(base_url, user, password, remote_path)
        assert code == 200 and blockmap.get("etag"), "missing initial ETag"

        code = delta_put_block(
            base_url, user, password, remote_path, 0, changed,
            if_match=blockmap["etag"])
        assert code == 200, f"staged block write returned {code}"

        # A reader between block staging and finalize must still observe the
        # committed file. The changed bytes become visible only at finalize.
        code, visible = webdav_get(base_url, user, password, remote_path)
        assert code == 200 and visible == original, \
            "reader observed staged bytes before finalize"
        ok("reader sees committed bytes while block is staged")

        code = delta_finalize(
            base_url, user, password, remote_path, len(changed),
            if_match=blockmap["etag"])
        assert code == 200, f"finalize returned {code}"
        code, visible = webdav_get(base_url, user, password, remote_path)
        assert code == 200 and visible == changed, \
            "reader did not observe committed bytes after finalize"
        ok("reader sees all staged bytes after finalize")
    except Exception as e:
        fail("staged finalize visibility", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


def test_inflight_reader_stress(base_url, user, password):
    print("\n[8] In-flight finalize reader stress")
    remote_path = "_delta_test_inflight_readers.bin"
    original = bytes([0x41]) * BLOCK_SIZE
    changed = bytes([0xD2]) * BLOCK_SIZE
    reader_barrier = threading.Barrier(9)
    observations = []
    errors = []
    observations_lock = threading.Lock()

    def reader() -> None:
        try:
            reader_barrier.wait(timeout=10)
            code, body = webdav_get(base_url, user, password, remote_path)
            with observations_lock:
                if code != 200:
                    errors.append(f"HTTP {code}")
                elif body == original:
                    observations.append("old")
                elif body == changed:
                    observations.append("new")
                else:
                    observations.append("mixed")
        except Exception as error:
            with observations_lock:
                errors.append(str(error))

    try:
        code = webdav_put(base_url, user, password, remote_path, original)
        assert code in (200, 201, 204), f"initial PUT returned {code}"
        code, blockmap = delta_blockmap(base_url, user, password, remote_path)
        assert code == 200 and blockmap.get("etag"), "missing initial ETag"

        for offset, block in ((0, changed),):
            code = delta_put_block(
                base_url, user, password, remote_path, offset, block,
                if_match=blockmap["etag"])
            assert code == 200, f"staged block at {offset} returned {code}"

        threads = [threading.Thread(target=reader, daemon=True) for _ in range(8)]
        for thread in threads:
            thread.start()
        # Release exactly eight readers together, then start finalize while
        # those requests are in flight. No new readers are spawned, so the
        # server can make progress once this bounded reader set drains.
        reader_barrier.wait(timeout=10)
        code = delta_finalize(
            base_url, user, password, remote_path, len(changed),
            if_match=blockmap["etag"])
        assert code == 200, f"finalize returned {code}"
        for thread in threads:
            thread.join(timeout=5)

        with observations_lock:
            assert observations, "reader threads produced no observations"
            assert not errors, f"reader errors: {errors[:3]}"
            assert "mixed" not in observations, \
                f"reader observed a mixed response: {observations[:10]}"
        ok(f"{len(observations)} concurrent reads saw only complete old/new files")
    except Exception as e:
        fail("in-flight reader stress", str(e))
    finally:
        webdav_delete(base_url, user, password, remote_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <base_url> <user> <password>")
        sys.exit(1)

    base_url = sys.argv[1].rstrip("/")
    user = sys.argv[2]
    password = sys.argv[3]

    print(f"Testing crispcloud_delta at {base_url} as {user}")

    test_status(base_url, user, password)
    test_blockmap_accuracy(base_url, user, password)
    test_delta_update(base_url, user, password)
    test_file_shrink(base_url, user, password)
    test_file_grow(base_url, user, password)
    test_stale_etag_rejected(base_url, user, password)
    test_staged_reader_visibility(base_url, user, password)
    test_inflight_reader_stress(base_url, user, password)

    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} passed")
    if passed < total:
        print("Failed:")
        for name, ok in results:
            if not ok:
                print(f"  - {name}")
        sys.exit(1)
