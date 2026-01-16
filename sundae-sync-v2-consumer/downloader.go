package syncV2Consumer

import (
	"encoding/hex"
	"fmt"
	"io"

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
}

// Download a block from the S3 bucket and return the bytes
func (h *S3Downloader) DownloadBlockSync(hash []byte) ([]byte, error) {
	prefix := fmt.Sprintf("%02x", hash[0])
	filename := fmt.Sprintf("blocks/by-hash/%v/%v.cbor", prefix, hex.EncodeToString(hash))
	resp, err := h.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(fmt.Sprintf("%v-sundae-sync-v2-%v-us-east-2", h.Network, h.Account)),
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
