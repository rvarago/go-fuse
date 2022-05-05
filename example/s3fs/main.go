// This program is exposes a list of files backed by an AWS S3 bucket where one **only** can list entries.
//
// For simplicity, the implementation eagerly caches metadata of all objects upon mounting and **never** refreshes it.
// Therefore, changes made to the bucket *after* mounting it into fs will not be visible to the latter.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// s3Bucket captures the intent to connect to a bucket.
type s3Bucket struct {
	fs.Inode

	name    string
	backend *s3.S3
}

// newS3Bucket creates a new s3 service on 'endpoint' for the given 'bucketName'.
func newS3Bucket(bucketName, endpoint string) (fs.InodeEmbedder, error) {
	session, err := session.NewSession(aws.NewConfig().WithEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("open session to failed: %v", err)
	}
	backend := s3.New(session, aws.NewConfig().WithS3ForcePathStyle(true))
	return &s3Bucket{name: bucketName, backend: backend}, nil
}

// OnAdd eagerly builds a fs view over the contents of the bucket.
func (b *s3Bucket) OnAdd(ctx context.Context) {
	if out, err := b.backend.ListObjects(&s3.ListObjectsInput{Bucket: &b.name}); err != nil {
		log.Printf("failed to query s3 bucket '%v': %v", b.name, err)
	} else {
		parent := &b.Inode
		for _, obj := range out.Contents {
			child := parent.NewPersistentInode(ctx, &s3Object{content: obj}, fs.StableAttr{})
			parent.AddChild(*obj.Key, child, true)
		}
	}
}

// s3Object is an entry in the bucket.
type s3Object struct {
	fs.Inode

	content *s3.Object
}

func (o *s3Object) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0444
	out.Nlink = 1
	out.Mtime = uint64(o.content.LastModified.Unix())
	out.Atime = uint64(0)
	out.Ctime = uint64(0)
	out.Size = uint64(*o.content.Size)
	out.Blksize = 0
	out.Blocks = 0
	return 0
}

func main() {
	bucketName := flag.String("bucket", "", "bucket name")

	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "oops! MOUNTPOINT was not provided.\n\nusage:\n  s3fs -bucket=BUCKET MOUNTPOINT")
		os.Exit(64)
	}

	if *bucketName == "" {
		fmt.Fprintf(os.Stderr, "oops! BUCKET was not provided.\n\nusage:\n  s3fs -bucket=BUCKET MOUNTPOINT")
		os.Exit(64)
	}

	mountpoint := flag.Arg(0)

	bucket, err := newS3Bucket(*bucketName, os.Getenv("AWS_ENDPOINT"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open connection to s3 bucket '%v': %v", bucketName, err)
		os.Exit(69)
	}

	server, err := fs.Mount(mountpoint, bucket, &fs.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to mount at '%v': %v", mountpoint, err)
		os.Exit(72)
	}
	log.Printf("mounting s3 bucket '%v' at '%v'", *bucketName, mountpoint)

	server.Wait()
}
