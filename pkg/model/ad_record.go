package model

type AdRecord struct {
	ID     string  `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8""`
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value  float32 `parquet:"name=value, type=FLOAT"`
	Reason string  `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	Sample int
	Size   int
}
