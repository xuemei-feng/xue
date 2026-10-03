#!/usr/bin/env python3
"""Standalone validation for initial-write exact-union parity slices."""

import unittest
from typing import Dict, Iterable, List, Sequence, Tuple


Interval = Tuple[int, int]
K = 6
R = 3
Z = 2
BLOCK_SIZE = 32


def gf_mul(a: int, b: int) -> int:
    """Multiply two bytes in GF(2^8) using the project's 0x1d reduction."""
    product = 0
    a &= 0xFF
    b &= 0xFF
    while b:
        if b & 1:
            product ^= a
        high_bit = a & 0x80
        a = (a << 1) & 0xFF
        if high_bit:
            a ^= 0x1D
        b >>= 1
    return product


def gf_gen_rs_matrix1(m: int, k: int) -> List[List[int]]:
    """Match gf_gen_rs_matrix1 in unilrc_encoder.cpp."""
    if m < k:
        raise ValueError("m must be at least k")
    matrix = [[0] * k for _ in range(m)]
    for row in range(k):
        matrix[row][row] = 1

    generator = 2
    for row in range(k, m):
        power = 1
        for column in range(k):
            matrix[row][column] = power
            power = gf_mul(power, generator)
        generator = gf_mul(generator, 2)
    return matrix


def azure_lrc_matrix(k: int, r: int, z: int) -> List[List[int]]:
    """Build identity, Vandermonde global parity, and local XOR rows."""
    if k <= 0 or r < 0 or z <= 0 or k % z:
        raise ValueError("k must be positive and divisible by z")
    matrix = gf_gen_rs_matrix1(k + r, k)
    group_size = k // z
    for local in range(z):
        row = [0] * k
        begin = local * group_size
        for block_id in range(begin, begin + group_size):
            row[block_id] = 1
        matrix.append(row)
    return matrix


def normalize_ranges(ranges: Iterable[Interval], stripe_size: int) -> List[Interval]:
    """Validate, sort, and merge overlapping or adjacent half-open ranges."""
    ordered = sorted(ranges)
    for begin, end in ordered:
        if begin < 0 or begin >= end or end > stripe_size:
            raise ValueError("range must be non-empty and inside the stripe")

    merged: List[Interval] = []
    for begin, end in ordered:
        if not merged or begin > merged[-1][1]:
            merged.append((begin, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def unique_byte_count(ranges: Iterable[Interval], stripe_size: int) -> int:
    return sum(end - begin for begin, end in normalize_ranges(ranges, stripe_size))


def split_ranges_by_block(
    logical_ranges: Iterable[Interval], k: int, block_size: int
) -> Dict[int, List[Interval]]:
    normalized = normalize_ranges(logical_ranges, k * block_size)
    by_block: Dict[int, List[Interval]] = {}
    for begin, end in normalized:
        position = begin
        while position < end:
            block_id = position // block_size
            block_offset = position % block_size
            length = min(end - position, block_size - block_offset)
            by_block.setdefault(block_id, []).append(
                (block_offset, block_offset + length)
            )
            position += length

    for block_id, intervals in list(by_block.items()):
        by_block[block_id] = normalize_ranges(intervals, block_size)
    return by_block


def exact_projection(by_block: Dict[int, List[Interval]], block_size: int) -> List[Interval]:
    intervals = [interval for pieces in by_block.values() for interval in pieces]
    return normalize_ranges(intervals, block_size) if intervals else []


def payload_byte(logical_offset: int) -> int:
    """Return reproducible content without random state or external fixtures."""
    return (logical_offset * 73 + 41) & 0xFF


def encode_sources(
    sources: Sequence[Sequence[int]], parity_rows: Sequence[Sequence[int]]
) -> List[bytearray]:
    if not sources:
        return [bytearray() for _ in parity_rows]
    width = len(sources[0])
    if any(len(source) != width for source in sources):
        raise ValueError("all sources must have equal length")

    parity = [bytearray(width) for _ in parity_rows]
    for parity_id, coefficients in enumerate(parity_rows):
        for block_id, coefficient in enumerate(coefficients):
            if coefficient == 0:
                continue
            source = sources[block_id]
            if coefficient == 1:
                for offset, value in enumerate(source):
                    parity[parity_id][offset] ^= value
            else:
                for offset, value in enumerate(source):
                    parity[parity_id][offset] ^= gf_mul(coefficient, value)
    return parity


def full_zero_stripe_reference(
    logical_ranges: Iterable[Interval], k: int, r: int, z: int, block_size: int
) -> List[bytearray]:
    """Fill selected bytes in zero data blocks, then encode complete blocks."""
    normalized = normalize_ranges(logical_ranges, k * block_size)
    sources = [bytearray(block_size) for _ in range(k)]
    for begin, end in normalized:
        for logical_offset in range(begin, end):
            block_id, block_offset = divmod(logical_offset, block_size)
            sources[block_id][block_offset] = payload_byte(logical_offset)

    matrix = azure_lrc_matrix(k, r, z)
    return encode_sources(sources, matrix[k:])


def exact_union_slice_encode(
    logical_ranges: Iterable[Interval], k: int, r: int, z: int, block_size: int
) -> Tuple[Dict[int, List[Interval]], List[Tuple[Interval, List[bytearray]]]]:
    """Encode each exact projected interval using zero-filled relative sources."""
    by_block = split_ranges_by_block(logical_ranges, k, block_size)
    projection = exact_projection(by_block, block_size)
    parity_rows = azure_lrc_matrix(k, r, z)[k:]
    encoded_slices: List[Tuple[Interval, List[bytearray]]] = []

    for q_begin, q_end in projection:
        width = q_end - q_begin
        sources = [bytearray(width) for _ in range(k)]
        for block_id, intervals in by_block.items():
            for begin, end in intervals:
                overlap_begin = max(begin, q_begin)
                overlap_end = min(end, q_end)
                for block_offset in range(overlap_begin, overlap_end):
                    relative_offset = block_offset - q_begin
                    logical_offset = block_id * block_size + block_offset
                    sources[block_id][relative_offset] = payload_byte(logical_offset)
        encoded_slices.append(((q_begin, q_end), encode_sources(sources, parity_rows)))

    return by_block, encoded_slices


class InitialRangeSliceTests(unittest.TestCase):
    def assert_case_matches_reference(
        self, name: str, ranges: Sequence[Interval]
    ) -> Tuple[Dict[int, List[Interval]], List[Interval]]:
        reference = full_zero_stripe_reference(ranges, K, R, Z, BLOCK_SIZE)
        by_block, encoded = exact_union_slice_encode(ranges, K, R, Z, BLOCK_SIZE)
        self.assertTrue(encoded, name)

        for q, parity_slices in encoded:
            q_begin, q_end = q
            self.assertEqual(q_end - q_begin, len(parity_slices[0]), name)
            self.assertEqual(R + Z, len(parity_slices), name)
            for parity_id, actual in enumerate(parity_slices):
                expected = reference[parity_id][q_begin:q_end]
                self.assertEqual(len(expected), len(actual), name)
                for relative_offset, (actual_byte, expected_byte) in enumerate(
                    zip(actual, expected)
                ):
                    self.assertEqual(
                        expected_byte,
                        actual_byte,
                        "%s parity=%d block_offset=%d"
                        % (name, parity_id, q_begin + relative_offset),
                    )
        return by_block, [q for q, _ in encoded]

    def test_six_required_range_shapes(self) -> None:
        cases = {
            "single_block_middle": [(5, 11)],
            "cross_block_boundary": [(BLOCK_SIZE - 3, BLOCK_SIZE + 4)],
            "multiple_discrete_in_one_block": [(2, 5), (10, 13)],
            "same_offset_in_multiple_blocks": [
                (2, 6),
                (BLOCK_SIZE + 2, BLOCK_SIZE + 6),
                (3 * BLOCK_SIZE + 2, 3 * BLOCK_SIZE + 6),
            ],
            "adjacent_and_overlapping": [(12, 15), (4, 9), (8, 12), (5, 7)],
            "non_prefix_block_ids": [
                (2 * BLOCK_SIZE + 3, 2 * BLOCK_SIZE + 8),
                (5 * BLOCK_SIZE + 14, 5 * BLOCK_SIZE + 18),
            ],
        }

        results = {}
        for name, ranges in cases.items():
            with self.subTest(name=name):
                results[name] = self.assert_case_matches_reference(name, ranges)

        _, discrete_q = results["multiple_discrete_in_one_block"]
        self.assertEqual([(2, 5), (10, 13)], discrete_q)
        self.assertNotEqual([(2, 13)], discrete_q)
        self.assertLess(sum(end - begin for begin, end in discrete_q), 13 - 2)

        same_offset_blocks, same_offset_q = results["same_offset_in_multiple_blocks"]
        self.assertEqual([0, 1, 3], sorted(same_offset_blocks))
        self.assertEqual([(2, 6)], same_offset_q)

        non_prefix_blocks, _ = results["non_prefix_block_ids"]
        self.assertEqual([2, 5], sorted(non_prefix_blocks))

    def test_payload_formula_is_deterministic(self) -> None:
        offsets = [0, 1, 2, 7, 31, 32, 191]
        expected = [41, 114, 187, 40, 0, 73, 160]
        self.assertEqual(expected, [payload_byte(offset) for offset in offsets])
        self.assertEqual(
            [payload_byte(offset) for offset in offsets],
            [payload_byte(offset) for offset in offsets],
        )

    def test_normalization_counts_unique_bytes(self) -> None:
        ranges = [(20, 25), (3, 8), (7, 12), (12, 15), (3, 8), (18, 22)]
        self.assertEqual([(3, 15), (18, 25)], normalize_ranges(ranges, K * BLOCK_SIZE))
        self.assertEqual(19, unique_byte_count(ranges, K * BLOCK_SIZE))
        self.assertLess(unique_byte_count(ranges, K * BLOCK_SIZE), sum(b - a for a, b in ranges))

    def test_gf_reduction_and_matrix_shape(self) -> None:
        self.assertEqual(0x1D, gf_mul(0x80, 2))
        self.assertEqual(gf_mul(0x53, 0xCA), gf_mul(0xCA, 0x53))
        matrix = azure_lrc_matrix(K, R, Z)
        self.assertEqual([1, 2, 4, 8, 16, 32], matrix[K])
        self.assertEqual([1, 4, 16, 64, 29, 116], matrix[K + 1])
        self.assertEqual([1, 1, 1, 0, 0, 0], matrix[K + R])
        self.assertEqual([0, 0, 0, 1, 1, 1], matrix[K + R + 1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
