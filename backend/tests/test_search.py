import json
import os
import sys

sys.path.append("..")  # Add parent directory to path

from utils.tokenizer import Tokenizer
from utils.file_io import read_json


class TestSearchEngine:
    def __init__(self):
        self.tokenizer = Tokenizer()
        # Mock inverted index data
        self.test_inverted_index = {
            "286975": {
                "docs": [
                    {"id": "31", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "64", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "88", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "103", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "119", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "156", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "204", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "223", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "232", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "243", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "250", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "267", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "277", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "289", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "304", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "331", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "405", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "430", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "494", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "510", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "514", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "627", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "772", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "831", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "849", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "859", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "876", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "883", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "973", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1023", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1032", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1041", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1049", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1053", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1063", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1070", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1118", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1132", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1177", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1184", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1192", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1216", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1224", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1225", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1238", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1240", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1260", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1274", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1359", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1395", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1431", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1442", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1457", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1463", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1469", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1534", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1535", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1553", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1557", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1583", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1601", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1618", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1639", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1648", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1653", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1740", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1772", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1773", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1881", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1887", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1892", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1898", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1915", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1917", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1919", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1925", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1941", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1943", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1981", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2115", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2117", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2127", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2205", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2220", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2245", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2255", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2288", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2297", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2311", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2364", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2438", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2447", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2504", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2521", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2534", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2567", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2620", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2657", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2697", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2699", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2702", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2832", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2833", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2852", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2878", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2920", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2991", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3039", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3070", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3074", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3109", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3208", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3214", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3242", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3272", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3403", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3455", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3469", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3516", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3548", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3557", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3615", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3643", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3682", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3696", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3704", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3767", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3793", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3820", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3830", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3840", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3888", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3894", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3912", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3913", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3951", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3955", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4033", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4036", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4041", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4055", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4068", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4126", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4152", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4230", "freq": 1, "positions": [0], "fields": ["name"]},
                ],
                "total_occurrences": 145,
                "field_occurrences": {"name": 145},
                "avg_position": 0.23448275862068965,
            },
            "286975": {
                "docs": [
                    {"id": "31", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "64", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "88", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "103", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "119", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "156", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "204", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "223", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "232", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "243", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "250", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "267", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "277", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "289", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "304", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "331", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "405", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "430", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "494", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "510", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "514", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "627", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "772", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "831", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "849", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "859", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "876", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "883", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "973", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1023", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1032", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1041", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1049", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1053", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1063", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1070", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1118", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1132", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1177", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1184", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1192", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1216", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1224", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1225", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1238", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1240", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1260", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1274", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1359", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1395", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1431", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1442", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1457", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1463", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1469", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1534", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1535", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1553", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1557", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1583", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1601", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1618", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1639", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1648", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1653", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1740", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1772", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1773", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1881", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1887", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1892", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1898", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1915", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1917", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1919", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1925", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1941", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1943", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1981", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2115", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2117", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2127", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2205", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2220", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2245", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2255", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2288", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2297", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2311", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2364", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2438", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2447", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2504", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2521", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2534", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2567", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2620", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2657", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2697", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2699", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2702", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2832", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2833", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2852", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2878", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2920", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2991", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3039", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3070", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3074", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3109", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3208", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3214", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3242", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3272", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3403", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3455", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3469", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3516", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3548", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3557", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3615", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3643", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3682", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3696", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3704", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3767", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3793", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3820", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3830", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3840", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3888", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3894", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3912", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "3913", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3951", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3955", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4033", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4036", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4041", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4055", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4068", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4126", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4152", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4230", "freq": 1, "positions": [0], "fields": ["name"]},
                ],
                "total_occurrences": 145,
                "field_occurrences": {"name": 145},
                "avg_position": 0.23448275862068965,
            },
            "685227": {
                "docs": [
                    {"id": "31", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "64", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "250", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "267", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "289", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "304", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "331", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "405", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "430", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "494", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "514", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "627", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "772", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "831", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "849", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "859", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "876", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "973", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1023", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1032", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1045",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1053", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1054",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1070", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1108",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {
                        "id": "1118",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {
                        "id": "1160",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1177", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1211",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1216", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1218",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1224", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1225", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1238", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1240", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "1256",
                        "freq": 1,
                        "positions": [2],
                        "fields": ["street-address"],
                    },
                    {"id": "1260", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1274", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1359", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1395", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1431", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1442", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1457", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1463", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1535", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1553", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1557", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1578", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1579", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "1583", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1618", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1639", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1648", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1653", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1772", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1881", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1887", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1892", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1898", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1915", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1917", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1919", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1925", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1941", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1943", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "1981", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2115", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2117", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2127", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2205", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2220", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2297", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2311", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2364", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "2410",
                        "freq": 1,
                        "positions": [1],
                        "fields": ["street-address"],
                    },
                    {"id": "2438", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2447", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "2451",
                        "freq": 1,
                        "positions": [1],
                        "fields": ["street-address"],
                    },
                    {"id": "2521", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2534", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2567", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2577", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2604", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2702", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2833", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2852", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2866", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "2920", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "2991", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3070", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3074", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3109", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "3113",
                        "freq": 1,
                        "positions": [1],
                        "fields": ["street-address"],
                    },
                    {"id": "3208", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3272", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3403", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3421", "freq": 1, "positions": [7], "fields": ["name"]},
                    {"id": "3455", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3469", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3516", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3548", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3557", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3643", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3682", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3696", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3704", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3767", "freq": 1, "positions": [1], "fields": ["name"]},
                    {
                        "id": "3770",
                        "freq": 1,
                        "positions": [1],
                        "fields": ["street-address"],
                    },
                    {"id": "3793", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3820", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3840", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3888", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3894", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3912", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "3986", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4033", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4041", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4055", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4068", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4120", "freq": 1, "positions": [0], "fields": ["name"]},
                    {"id": "4152", "freq": 1, "positions": [1], "fields": ["name"]},
                    {"id": "4230", "freq": 1, "positions": [1], "fields": ["name"]},
                ],
                "total_occurrences": 122,
                "field_occurrences": {"name": 110, "street-address": 12},
                "avg_position": 1.0901639344262295,
            },
        }
        self.lexicon = self._load_lexicon()

    def _load_lexicon(self):
        """Load the lexicon and create reverse mapping"""
        lexicon_path = "../index data/lexicon/lexicon.json"
        lexicon = read_json(lexicon_path)
        return {str(v): k for k, v in lexicon.items()}  # Reverse mapping: ID -> word

    def decode_document(self, doc_id="1"):
        """Decode a document's content using the lexicon"""
        doc = self.test_data[doc_id]

        # Decode each field
        decoded = {"name": [], "locality": [], "street-address": [], "region": []}

        # Decode words and their positions
        word_positions = {}
        for word_id, positions in doc["word_positions"].items():
            if word_id in self.lexicon:
                word = self.lexicon[word_id]
                word_positions[word] = positions

        # Decode field matches
        for field, word_ids in doc["field_matches"].items():
            for word_id in word_ids:
                if str(word_id) in self.lexicon:
                    decoded[field].append(self.lexicon[str(word_id)])

        return {
            "decoded_fields": decoded,
            "word_positions": word_positions,
            "total_words": doc["total_words"],
        }

    def test_search(self, query):
        """Test search functionality with new inverted index format"""
        # Get both original and tokenized forms
        original_words = query.lower().split()
        tokenized_words = self.tokenizer.tokenize_with_spacy(query.lower())
        query_tokens = list(set(original_words + tokenized_words))

        print(f"\nSearch test for: {query}")
        print(f"Original words: {original_words}")
        print(f"Tokenized words: {tokenized_words}")

        # Find matching documents and calculate scores
        matched_docs = {}

        # Process each token
        for token in query_tokens:
            if token in self.lexicon:
                word_id = str(self.lexicon[token])
                if word_id in self.test_inverted_index:
                    token_info = self.test_inverted_index[word_id]

                    # Process each document
                    for doc in token_info["docs"]:
                        doc_id = doc["id"]

                        # Calculate score based on our scoring formula
                        score = self._calculate_score(doc, token_info)

                        if doc_id not in matched_docs:
                            matched_docs[doc_id] = {
                                "score": score,
                                "matched_tokens": 1,
                                "matches": [
                                    {
                                        "token": token,
                                        "freq": doc["freq"],
                                        "fields": doc["fields"],
                                        "positions": doc["positions"],
                                    }
                                ],
                            }
                        else:
                            matched_docs[doc_id]["score"] += score
                            matched_docs[doc_id]["matched_tokens"] += 1
                            matched_docs[doc_id]["matches"].append(
                                {
                                    "token": token,
                                    "freq": doc["freq"],
                                    "fields": doc["fields"],
                                    "positions": doc["positions"],
                                }
                            )

        # Sort results by score
        sorted_results = sorted(
            matched_docs.items(), key=lambda x: x[1]["score"], reverse=True
        )

        return {
            "query": query,
            "total_matches": len(sorted_results),
            "results": sorted_results[:10],  # Top 10 results
        }

    def _calculate_score(self, doc, token_info):
        """Calculate relevance score for a document"""
        score = 0

        # Base frequency score
        score += doc["freq"] * 0.3

        # Field weights
        field_weights = {
            "name": 3.0,
            "title": 2.5,
            "text": 1.0,
            "locality": 1.5,
            "region": 1.0,
            "street-address": 1.0,
        }

        for field in doc["fields"]:
            score += field_weights.get(field, 1.0)

        # Position bonus
        if doc["positions"]:
            score += max(0, 1 - (doc["positions"][0] / 100)) * 0.5

        return score


def main():
    engine = TestSearchEngine()

    # Test queries
    test_queries = ["western", "best", "well", "good"]

    for query in test_queries:
        result = engine.test_search(query)
        print("\nResults for:", query)
        print(f"Total matches: {result['total_matches']}")
        print("\nTop matches:")
        for doc_id, info in result["results"][:3]:  # Show top 3
            print(f"\nDoc ID: {doc_id}")
            print(f"Score: {info['score']:.2f}")
            print(f"Matched tokens: {info['matched_tokens']}")
            print("Matches:", info["matches"])


if __name__ == "__main__":
    main()
