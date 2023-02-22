package files

import "os"

func ExistFile(filePath string) (ok bool) {
	_, err := os.Stat(filePath)
	if err == nil {
		ok = true
		return
	}
	if os.IsNotExist(err) {
		return
	}
	ok = true
	return
}
