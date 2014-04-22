package main

import (
	"compress/gzip"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/bobveznat/goamz/s3"
	"github.com/mitchellh/goamz/aws"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var content_type_map = map[string]string{
	"css":  "text/css",
	"html": "text/html",
	"htm":  "text/html",
	"ico":  "image/x-ico",
	"js":   "text/javascript",
	"jpg":  "image/jpeg",
	"JPG":  "image/jpeg",
	"gif":  "image/gif",
	"GIF":  "image/gif",
	"png":  "image/png",
	"PNG":  "image/png",
}

type FileInfo struct {
	absolute_path   string
	compressed_path string
	os_fileinfo     os.FileInfo
}

func main() {

	var source_path = flag.String("source-path", "", "Source directory")
	var dest_bucket = flag.String("bucket", "", "Bucket in S3")
	var region_name = flag.String("region", "", "AWS region of the bucket")
	flag.Parse()
	if *source_path == "" || *dest_bucket == "" {
		log.Println("Missing -source-path or -bucket")
		flag.Usage()
		os.Exit(1)
	}
	if *region_name == "" {
		*region_name = "us-east-1"
	}
	creds, err := aws.EnvAuth()
	if err != nil {
		log.Println("I messed up:", err)
		os.Exit(1)
	}

	region := aws.Regions[*region_name]
	s3_conn := s3.New(creds, region)
	bucket := s3_conn.Bucket(*dest_bucket)
	bucket.PutBucket(s3.PublicRead)
	if bucket == nil {
		log.Println("no bucket?")
		os.Exit(1)
	}

	s3_keys, _ := bucket.GetBucketContents()
	all_files := make(chan *FileInfo, 100)
	done_channel := make(chan int)
	go get_all_files(*source_path, all_files, true)
	num_uploaders := 4
	for i := 0; i < num_uploaders; i++ {
		go process_all_files(*source_path, all_files, bucket, s3_keys, done_channel)
	}
	for i := 0; i < num_uploaders; i++ {
		<-done_channel
	}
}

func hash_file(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Println("Could not open %v: %v", filename, err)
		return "", err
	}
	defer file.Close()

	hasher := md5.New()
	io.Copy(hasher, file)
	hash_val := fmt.Sprintf("%x", hasher.Sum(nil))
	return hash_val, nil
}

func process_all_files(source_path string, all_files chan *FileInfo, bucket *s3.Bucket, s3_keys *map[string]s3.Key, done_channel chan int) {
	for file_info := range all_files {
		if file_info == nil {
			break
		}
		log.Println(file_info.absolute_path)

		headers := map[string][]string{}
		headers["Cache-Control"] = []string{"max-age=900"}

		dot_idx := 1 + strings.LastIndex(file_info.absolute_path, ".")
		suffix := file_info.absolute_path[dot_idx:]
		if suffix != "jpg" && suffix != "gif" && suffix != "png" && suffix != "JPG" && suffix != "GIF" && suffix != "PNG" {
			log.Println("\tcompressing", suffix)
			compressed_file, err := ioutil.TempFile("", "s3uploader")
			if err != nil {
				log.Println("\tCouldn't get a temp file", err)
				continue
			}
			gzipper, _ := gzip.NewWriterLevel(compressed_file, gzip.BestCompression)
			file, err := os.Open(file_info.absolute_path)
			if err != nil {
				log.Println("\tCouldn't open original file", file_info.absolute_path, err)
				continue
			}
			io.Copy(gzipper, file)
			file.Close()
			gzipper.Close()
			file_info.compressed_path = compressed_file.Name()

			content_type_str, ok := content_type_map[suffix]
			if !ok {
				content_type_str = "application/octet-stream"
				log.Println("\tUnknown extension:", file_info.absolute_path)
			}
			headers["Content-Type"] = []string{content_type_str}
			headers["Content-Encoding"] = []string{"gzip"}
		} else {
			content_type_str, ok := content_type_map[suffix]
			if !ok {
				content_type_str = "application/octet-stream"
				log.Println("\tUnknown extension:", file_info.absolute_path)
			}
			headers["Content-Type"] = []string{content_type_str}
		}

		var path_to_contents string
		if len(file_info.compressed_path) > 0 {
			path_to_contents = file_info.compressed_path
		} else {
			path_to_contents = file_info.absolute_path
		}
		hash, err := hash_file(path_to_contents)
		if err != nil {
			log.Printf("\tCouldn't do hash for %s: %v\n",
				file_info.absolute_path, err)
		}
		key_name := file_info.absolute_path[len(source_path)+1:]
		key, ok := (*s3_keys)[key_name]
		// this library returns the ETag with quotes around it, we strip them
		if ok && key.ETag[1:len(key.ETag)-1] == hash {
			log.Println("\thashes match, no upload required", key_name)
			continue
		} else {
			log.Println("\tUploading", key_name)
		}

		info, _ := os.Stat(path_to_contents)
		file, err := os.Open(path_to_contents)
		if err != nil {
			log.Printf("Can't open file %s: %v\n", path_to_contents, err)
		}
		bucket.PutReaderHeader(key_name, file, info.Size(),
			headers, s3.PublicRead)
		file.Close()
		if len(file_info.compressed_path) > 0 {
			os.Remove(file_info.compressed_path)
		}
		log.Println("\tFinished upload")

	}
	done_channel <- 1
}

func get_all_files(dirname string, all_files chan *FileInfo, first_call bool) {
	file, err := os.Open(dirname)
	if err != nil {
		log.Fatalf("Could not cd to %s (%v), aborting.\n", dirname, err)
	}
	fileinfos, err := file.Readdir(0)
	if err != nil {
		log.Fatal("Couldn't read dir")
	}
	for _, fileinfo := range fileinfos {
		full_path := fmt.Sprintf("%s/%s", dirname, fileinfo.Name())
		if fileinfo.IsDir() {
			get_all_files(full_path, all_files, false)
		} else {
			this_file := new(FileInfo)
			this_file.absolute_path = full_path
			this_file.os_fileinfo = fileinfo
			all_files <- this_file
		}
	}
	if first_call {
		close(all_files)
	}
}
