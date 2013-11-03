s3-website-sync
===============

Will sync a static website up to S3. Compresses HTML and CSS using gzip before
uploading. Does not compress images. Compares destination files to source files
to avoid uploading files when they contain the same exact data (this is why its
a sync not a copy).

To build this you'll need to:

    go get github.com/mitchellh/goamz/aws
    go get github.com/mitchellh/goamz/s3


# Usage

    Usage of ./s3-website-sync:
      -bucket="": The name of the destination bucket in S3
      -source-path="": The path to the source directory containing the website
