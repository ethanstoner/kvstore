package com.ethanstoner.kvstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class BloomFilter {

    private final byte[] bits;
    private final int numHashFunctions;

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions < 1) expectedInsertions = 1;
        int bitCount = optimalBitCount(expectedInsertions, falsePositiveRate);
        this.bits = new byte[(bitCount + 7) / 8];
        this.numHashFunctions = optimalHashCount(bitCount, expectedInsertions);
    }

    private BloomFilter(byte[] bits, int numHashFunctions) {
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
    }

    public void add(String key) {
        long hash64 = murmur3Hash64(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);
        int bitCount = bits.length * 8;
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = h1 + i * h2;
            int pos = (combinedHash & Integer.MAX_VALUE) % bitCount;
            bits[pos / 8] |= (byte) (1 << (pos % 8));
        }
    }

    public boolean mightContain(String key) {
        long hash64 = murmur3Hash64(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);
        int bitCount = bits.length * 8;
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = h1 + i * h2;
            int pos = (combinedHash & Integer.MAX_VALUE) % bitCount;
            if ((bits[pos / 8] & (1 << (pos % 8))) == 0) return false;
        }
        return true;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(bits.length);
        out.write(bits);
        out.writeInt(numHashFunctions);
    }

    public static BloomFilter readFrom(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bits = new byte[len];
        in.readFully(bits);
        int numHash = in.readInt();
        return new BloomFilter(bits, numHash);
    }

    private static int optimalBitCount(int n, double fpp) {
        return (int) Math.ceil(-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalHashCount(int bitCount, int n) {
        return Math.max(1, (int) Math.ceil((double) bitCount / n * Math.log(2)));
    }

    private static long murmur3Hash64(String key) {
        byte[] data = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long h1 = 0L;
        long h2 = 0L;
        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        int nblocks = data.length / 16;
        for (int i = 0; i < nblocks; i++) {
            long k1 = getLong(data, i * 16);
            long k2 = getLong(data, i * 16 + 8);
            k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
            k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        int tail = nblocks * 16;
        long k1 = 0, k2 = 0;
        switch (data.length - tail) {
            case 15: k2 ^= (long)(data[tail+14] & 0xff) << 48;
            case 14: k2 ^= (long)(data[tail+13] & 0xff) << 40;
            case 13: k2 ^= (long)(data[tail+12] & 0xff) << 32;
            case 12: k2 ^= (long)(data[tail+11] & 0xff) << 24;
            case 11: k2 ^= (long)(data[tail+10] & 0xff) << 16;
            case 10: k2 ^= (long)(data[tail+9] & 0xff) << 8;
            case 9:  k2 ^= (long)(data[tail+8] & 0xff);
                     k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
            case 8:  k1 ^= (long)(data[tail+7] & 0xff) << 56;
            case 7:  k1 ^= (long)(data[tail+6] & 0xff) << 48;
            case 6:  k1 ^= (long)(data[tail+5] & 0xff) << 40;
            case 5:  k1 ^= (long)(data[tail+4] & 0xff) << 32;
            case 4:  k1 ^= (long)(data[tail+3] & 0xff) << 24;
            case 3:  k1 ^= (long)(data[tail+2] & 0xff) << 16;
            case 2:  k1 ^= (long)(data[tail+1] & 0xff) << 8;
            case 1:  k1 ^= (long)(data[tail] & 0xff);
                     k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
        }

        h1 ^= data.length; h2 ^= data.length;
        h1 += h2; h2 += h1;
        h1 = fmix64(h1); h2 = fmix64(h2);
        h1 += h2;
        return h1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33; k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33; k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLong(byte[] data, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (long)(data[offset + i] & 0xff) << (i * 8);
        }
        return v;
    }
}
