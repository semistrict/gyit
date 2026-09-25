#!/usr/bin/env python3
"""Extract a bounded native-delta benchmark fixture from an existing SHA-1 pack.

No clone, fetch, import, or source modification. Sample at most 512 blob targets
from 64 spaced windows; Go tests verify reconstructed bytes against Git OIDs.
Requires the pack's existing .idx and .rev files. The watchdog is 15 seconds.
"""
import argparse
import functools
import os
from pathlib import Path
import signal
import struct
import time
import zlib

MAX_OBJECT = 256 << 10
MAX_FIXTURE = 64 << 20


class TooLarge(Exception):
    pass


def integer(data, pos):
    value = 0
    for shift in range(0, 70, 7):
        if pos == len(data):
            raise ValueError('truncated delta size')
        byte = data[pos]
        pos += 1
        value |= (byte & 127) << shift
        if not byte & 128:
            return value, pos
    raise ValueError('overflowing delta size')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pack', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    start = time.monotonic()

    def timeout(*_):
        raise TimeoutError('native delta sample exceeded 15 seconds')

    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(15)
    with args.pack.open('rb') as packed, args.pack.with_suffix('.idx').open('rb') as indexed, args.pack.with_suffix('.rev').open('rb') as reversed_index:
        def read(file, n, offset):
            data = os.pread(file.fileno(), n, offset)
            if len(data) != n:
                raise ValueError('truncated pack or index')
            return data

        if read(packed, 8, 0) != b'PACK\x00\x00\x00\x02':
            raise ValueError('requires a v2 pack')
        if read(indexed, 8, 0) != b'\xfftOc\x00\x00\x00\x02':
            raise ValueError('requires a v2 pack index')
        if read(reversed_index, 12, 0) != b'RIDX\x00\x00\x00\x01\x00\x00\x00\x01':
            raise ValueError('requires a v1 SHA-1 reverse index')
        count = struct.unpack('>I', read(indexed, 4, 8 + 255 * 4))[0]
        if count != struct.unpack('>I', read(packed, 4, 8))[0] or count == 0:
            raise ValueError('pack/index object counts differ or are empty')
        offset_table = 8 + 1024 + count * 24
        large_table = offset_table + count * 4

        def object_offset(position):
            if position >= count:
                raise ValueError('reverse index position outside index')
            offset = struct.unpack('>I', read(indexed, 4, offset_table + position * 4))[0]
            if offset & 0x80000000:
                offset = struct.unpack('>Q', read(indexed, 8, large_table + 8 * (offset & 0x7fffffff)))[0]
            return offset

        @functools.lru_cache(maxsize=32768)
        def header(offset):
            data = read(packed, 32, offset)
            byte, pos = data[0], 1
            kind, size, shift = (byte >> 4) & 7, byte & 15, 4
            while byte & 128:
                if shift > 63 or pos >= len(data):
                    raise ValueError('overflowing object size')
                byte = data[pos]
                pos += 1
                size |= (byte & 127) << shift
                shift += 7
            base = None
            if kind == 6:
                byte = data[pos]
                pos += 1
                distance = byte & 127
                while byte & 128:
                    if pos >= len(data):
                        raise ValueError('overflowing delta offset')
                    byte = data[pos]
                    pos += 1
                    distance = ((distance + 1) << 7) + (byte & 127)
                base = offset - distance
                if base < 12 or base >= offset:
                    raise ValueError('delta does not point backward into pack')
            elif kind == 7:
                raise ValueError('sample supports OFS_DELTA packs only')
            elif kind not in (1, 2, 3, 4):
                raise ValueError('invalid object kind')
            return kind, size, base, offset + pos

        @functools.lru_cache(maxsize=32768)
        def identity(offset):
            kind, _, base, _ = header(offset)
            if base is None:
                return kind, 0
            kind, depth = identity(base)
            if depth >= 64:
                raise TooLarge()
            return kind, depth + 1

        @functools.lru_cache(maxsize=64)
        def payload(offset):
            kind, size, _, pos = header(offset)
            if size > MAX_OBJECT:
                raise TooLarge()
            decoder = zlib.decompressobj()
            output = bytearray()
            while not decoder.eof:
                block = os.pread(packed.fileno(), 4096, pos)
                if not block:
                    raise ValueError('truncated zlib frame')
                pos += len(block)
                output.extend(decoder.decompress(block, MAX_OBJECT + 1 - len(output)))
                if len(output) > MAX_OBJECT:
                    raise TooLarge()
            if len(output) != size:
                raise ValueError('inflated size differs from pack header')
            if kind == 6:
                base_size, pos = integer(output, 0)
                target_size, _ = integer(output, pos)
                if max(base_size, target_size) > MAX_OBJECT:
                    raise TooLarge()
            return bytes(output)

        args.output.mkdir(parents=True, exist_ok=True)
        nodes, cases, selected = {}, [], set()
        fixture_bytes = 0
        window = min(256, count)
        for block in range(64):
            begin = min(count - window, block * count // 64)
            positions = struct.unpack(f'>{window}I', read(reversed_index, window * 4, 12 + begin * 4))
            taken = 0
            for position in positions:
                offset = object_offset(position)
                if offset in selected:
                    continue
                try:
                    kind, depth = identity(offset)
                    if kind != 3 or depth == 0:
                        continue
                    chain, current = [], offset
                    while True:
                        _, _, base, _ = header(current)
                        chain.append((current, base, payload(current)))
                        if base is None:
                            break
                        current = base
                except TooLarge:
                    continue
                oid = read(indexed, 20, 8 + 1024 + position * 20).hex()
                for current, base, data in chain:
                    if current in nodes:
                        continue
                    fixture_bytes += len(data)
                    if len(nodes) >= 10000 or fixture_bytes > MAX_FIXTURE:
                        raise ValueError('sample exceeds bounded fixture budget')
                    (args.output / f'{current}.bin').write_bytes(data)
                    nodes[current] = base if base is not None else -1
                cases.append((offset, chain[-1][0], oid))
                selected.add(offset)
                taken += 1
                if taken == 8:
                    break
        if not cases:
            raise ValueError('no eligible native blob deltas in sample')
        (args.output / 'nodes.tsv').write_text('offset\tbase\n' + ''.join(f'{offset}\t{base}\n' for offset, base in sorted(nodes.items())))
        (args.output / 'cases.tsv').write_text('offset\troot\toid\n' + ''.join(f'{offset}\t{root}\t{oid}\n' for offset, root, oid in cases))
        print(f'{len(cases)} targets, {len(nodes)} nodes, {fixture_bytes} bytes, {time.monotonic()-start:.3f}s')
    signal.alarm(0)


if __name__ == '__main__':
    main()
