#!/usr/bin/env python3
import zlib
import sys

filename = sys.argv[1]
compressed = open(filename, 'rb').read()
raw = zlib.decompress(compressed)
print(raw)
