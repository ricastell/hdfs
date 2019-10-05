package hdfs

import (
	"os"

	"github.com/golang/protobuf/proto"
	hdfs "github.com/ricastell/hdfs/v2/internal/protocol/hadoop_hdfs"
)

type QuotaUsage struct {
	treeRoot string
	usage    *hdfs.QuotaUsageProto
}

// Quota returns the quota for the tree with root path.
func (c *Client) Quota(path string) (*QuotaUsage, error) {
	qu, err := c.getQuota(path)
	if err != nil {
		err = &os.PathError{"quota", path, interpretException(err)}
	}

	return qu, err
}

func (c *Client) getQuota(path string) (*QuotaUsage, error) {
	req := &hdfs.GetQuotaUsageRequestProto{
		Path: proto.String(path),
	}
	resp := &hdfs.GetQuotaUsageResponseProto{}

	err := c.namenode.Execute("getQuotaUsage", req, resp)
	if err != nil {
		return newQuotaUsage(nil, path), os.ErrNotExist
	}
	return newQuotaUsage(resp.GetUsage(), path), nil
}

func newQuotaUsage(usage *hdfs.QuotaUsageProto, path string) *QuotaUsage {
	qu := &QuotaUsage{
		treeRoot: path,
		usage:    usage}
	return qu
}

func (qu *QuotaUsage) FilesCount() uint64 {
	return qu.usage.GetFileAndDirectoryCount()
}

func (qu *QuotaUsage) FilesQuota() uint64 {
	return qu.usage.GetQuota()
}

func (qu *QuotaUsage) SpaceConsumed() uint64 {
	return qu.usage.GetSpaceConsumed()
}

func (qu *QuotaUsage) SpaceQuota() uint64 {
	return qu.usage.GetSpaceQuota()
}

func (c *Client) SetQuota(path string, nsquota uint64, spacequota uint64) error {

	req := &hdfs.SetQuotaRequestProto{
		Path:              proto.String(path),
		NamespaceQuota:    proto.Uint64(nsquota),
		StoragespaceQuota: proto.Uint64(spacequota),
	}
	resp := &hdfs.SetQuotaResponseProto{}

	err := c.namenode.Execute("setQuota", req, resp)
	if err != nil {
		return err
	}
	return nil
}
