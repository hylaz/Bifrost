package storage

import "time"

func GetKeyVal(key []byte) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = xdbClient.GetKeyValBytes(DEFAULT_TABLE, string(key))
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}

func PutKeyVal(key []byte, val []byte) (err error) {
	for i := 0; i < 3; i++ {
		err = xdbClient.PutKeyValBytes(DEFAULT_TABLE, string(key), val)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}

func DelKeyVal(key []byte) (err error) {
	for i := 0; i < 3; i++ {
		err = xdbClient.DelKeyVal(DEFAULT_TABLE, string(key))
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return
}
