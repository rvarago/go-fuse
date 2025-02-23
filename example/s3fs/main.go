// This program exposes a FUSE backed by an aws s3 bucket where one can **only** list objects contained in the bucket.
//
// For simplicity, the implementation eagerly caches metadata of all objects upon mounting and **never** refreshes it.
// Therefore, changes made to the bucket *after* mounting it into fs will not be visible to the latter.
//
// # Possible improvements
//
// 1. Support fetching s3 on demand, with a cli flag to cache it,
// 2. Bound fs operations to a sensible timeout,
// 3. Add other relevant fs operations,
// 4. Add support for auto-umount.
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

// Exit status as per https://www.freebsd.org/cgi/man.cgi?query=sysexits.
const (
	EXUSAGE       = 64
	EXUNAVAILABLE = 69
	EXOSFILE      = 72
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
		return nil, fmt.Errorf("failed to establish session with s3: %v", err)
	}
	backend := s3.New(session, aws.NewConfig().WithS3ForcePathStyle(true))
	return &s3Bucket{name: bucketName, backend: backend}, nil
}

// OnAdd eagerly builds an fs view over the contents of the bucket.
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
	out.Mode = 0444 // -r--r--r--
	out.Nlink = 1
	out.Mtime = uint64(o.content.LastModified.Unix())
	out.Atime = uint64(0)
	out.Ctime = uint64(0)
	out.Size = uint64(*o.content.Size)
	out.Blksize = 0
	out.Blocks = 0
	return 0
}

// cli is the set of options to start up this app.
type cli struct {
	mountPoint string
	bucketName string
	endpoint   string
}

// newCli exposes the command-line interface to users.
func newCli() cli {
	bucketName := flag.String("bucket", "", "bucket name")

	flag.Parse()

	bailIf := func(check bool, cause string) {
		if check {
			fmt.Fprintf(os.Stderr, "oops! %v.\n\nusage:\n  s3fs -bucket=BUCKET MOUNTPOINT", cause)
			os.Exit(EXUSAGE)
		}
	}

	bailIf(len(flag.Args()) < 1, "MOUNTPOINT was not provided")
	bailIf(*bucketName == "", "BUCKET was not provided")

	return cli{
		mountPoint: flag.Arg(0),
		bucketName: *bucketName,
		endpoint:   os.Getenv("AWS_ENDPOINT"),
	}
}

func main() {
	cli := newCli()

	bucket, err := newS3Bucket(cli.bucketName, cli.endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open s3 connection to bucket '%v': %v", cli.bucketName, err)
		os.Exit(EXUNAVAILABLE)
	}

	server, err := fs.Mount(cli.mountPoint, bucket, &fs.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to mount at '%v': %v", cli.mountPoint, err)
		os.Exit(EXOSFILE)
	}
	log.Printf("mounted s3 bucket '%v' at '%v'", cli.bucketName, cli.mountPoint)

	server.Wait()
}
