package syncV2Consumer

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/rs/zerolog"
)

type Downloader interface {
	DownloadBlockSync(hash []byte) ([]byte, error)
	DownloadBlock(hash []byte, ch chan []byte) error
}

type S3Downloader struct {
	Logger  zerolog.Logger
	Network string
	Account string
	S3      s3iface.S3API
	// Bucket overrides the default `{Network}-sundae-sync-v2-{Account}-us-east-2`
	// bucket. Accepts a bare bucket name or an `s3://name` URI. Empty falls
	// back to the conventional interpolation.
	Bucket string
}

// bucketName returns the bucket to download from, honouring an explicit
// override and stripping any `s3://` prefix.
func (h *S3Downloader) bucketName() string {
	if h.Bucket != "" {
		return strings.TrimPrefix(h.Bucket, "s3://")
	}
	return fmt.Sprintf("%v-sundae-sync-v2-%v-us-east-2", h.Network, h.Account)
}

// Download a block from the S3 bucket and return the bytes
func (h *S3Downloader) DownloadBlockSync(hash []byte) ([]byte, error) {
	prefix := fmt.Sprintf("%02x", hash[0])
	filename := fmt.Sprintf("blocks/by-hash/%v/%v.cbor", prefix, hex.EncodeToString(hash))
	resp, err := h.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(h.bucketName()),
		Key:    aws.String(filename),
	})
	if err != nil {
		h.Logger.Warn().Str("filename", filename).Err(err).Msg("Failed downloading block")
		return nil, err
	}
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		h.Logger.Warn().Str("filename", filename).Err(err).Msg("Failed reading block contents")
		return nil, err
	}
	return bytes, nil
}

// Download a block from the S3 bucket and deliver it on a channel
func (h *S3Downloader) DownloadBlock(hash []byte, ch chan []byte) error {
	bytes, err := h.DownloadBlockSync(hash)
	if err != nil {
		return err
	}
	ch <- bytes
	return nil
}
