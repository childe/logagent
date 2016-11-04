package main

import "os"

func openfile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.Open(name)
}
