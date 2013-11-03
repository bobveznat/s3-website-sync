package main

import (
    "compress/gzip"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io"
    "io/ioutil"
	"log"
	"os"
    "strings"
)

var content_type_map = map[string]string {
    "css": "text/css",
    "html": "text/html",
    "js": "text/javascript",
}

func get_s3_dir(bucket *s3.Bucket, directory_path string, s3_contents *map[string]s3.Key) {
	contents, err := bucket.List(directory_path, "/", "/", 1024)
	if err != nil {
		log.Print("no listing?", err)
		os.Exit(1)
	}
	for _, key := range contents.Contents {
        (*s3_contents)[key.Key] = key
	}
	for _, subdir := range contents.CommonPrefixes {
		get_s3_dir(bucket, subdir, s3_contents)
	}
}

type FileInfo struct {
	absolute_path string
	compressed_path string
	os_fileinfo   os.FileInfo
}

func main() {

	var source_path = flag.String("source-path", "", "Source directory")
	var dest_bucket = flag.String("bucket", "", "Bucket in S3")
	flag.Parse()
	if *source_path == "" || *dest_bucket == "" {
		log.Println("Missing -source-path or -bucket")
		flag.Usage()
		os.Exit(1)
	}
	creds, err := aws.EnvAuth()
	if err != nil {
		log.Println("I messed up:", err)
		os.Exit(1)
	}

	region := aws.Regions["us-east-1"]
	s3_conn := s3.New(creds, region)
	bucket := s3_conn.Bucket(*dest_bucket)
	if bucket == nil {
		log.Println("no bucket?")
		os.Exit(1)
	}

    s3_keys := map[string]s3.Key {}
	get_s3_dir(bucket, "", &s3_keys)

	all_files := make(chan *FileInfo, 10)
	go get_all_files(*source_path, all_files, true)
	process_all_files(*source_path, all_files, bucket, s3_keys)
}

func hash_file(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Println("Could not open %v: %v", filename, err)
		return "", err
	}
	defer file.Close()

	hasher := md5.New()
    io.Copy(hasher,file)
	hash_val := fmt.Sprintf("%x", hasher.Sum(nil))
	return hash_val, nil
}

func process_all_files(source_path string, all_files chan *FileInfo, bucket *s3.Bucket, s3_keys map[string]s3.Key) {
	for file_info := range all_files {
		if file_info == nil {
			break
		}
		log.Println(file_info.absolute_path)

        headers := map[string][]string {}
        var cache_control []string
        cache_control = append(cache_control, "max-age=900")
        headers["Cache-Control"] = cache_control

        dot_idx := 1 + strings.LastIndex(file_info.absolute_path, ".")
        suffix := file_info.absolute_path[dot_idx:]
        if suffix != "jpg" && suffix != "gif" && suffix != "png" {
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

            var content_encoding []string
            content_encoding = append(content_encoding, "gzip")
            var content_type []string
            content_type_str, ok := content_type_map[suffix]
            if !ok {
                content_type_str = "application/octet-stream"
                log.Println("\tUnknown extension:", file_info.absolute_path)
            }
            content_type = append(content_type, content_type_str)
            headers["Content-Type"] = content_type
            headers["Content-Encoding"] = content_encoding
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
        key_name := file_info.absolute_path[len(source_path) + 1:]
        key, ok := s3_keys[key_name]
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
		all_files <- nil
	}
}
