package writer

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
)

type File struct {
	fileName string
}

func NewFile(fileName string) (f *File, err error) {
	return &File{
		fileName: fileName,
	}, err
}

func (c *File) Write(f string, r *bufio.Reader) (err error) {
	outFile, err := os.Create(f)
	defer outFile.Close()
	w := gzip.NewWriter(outFile)
	_, err = r.WriteTo(w)
	w.Close()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	return nil
}
