package main

import (
	"context"
	"os"
	"time"

	"github.com/0gfoundation/0g-storage-client/common"
	"github.com/0gfoundation/0g-storage-client/common/blockchain"
	"github.com/0gfoundation/0g-storage-client/core"
	"github.com/0gfoundation/0g-storage-client/indexer"
	"github.com/0gfoundation/0g-storage-client/transfer"
	"github.com/sirupsen/logrus"
)

func main() {
	// 配置参数
	blockchainRPC := "https://evmrpc-testnet.0g.ai/"
	indexerURL := "https://indexer-storage-testnet-turbo.0g.ai"
	filePath := "DISC-Law-SFT-Pair-QA-released.jsonll"

	privateKey := os.Getenv("PRIVATE_KEY")

	fragmentSize := int64(20 * 1024 * 1024)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 初始化 Web3 客户端
	w3client := blockchain.MustNewWeb3(blockchainRPC, privateKey)
	defer w3client.Close()

	// 初始化 Indexer 客户端
	indexerClient, err := indexer.NewClient(indexerURL, indexer.IndexerClientOption{
		LogOption: common.LogOption{Logger: logrus.StandardLogger()},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize indexer client")
	}
	defer indexerClient.Close()

	// 打开文件
	file, err := core.Open(filePath)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to open file")
	}
	defer file.Close()

	// 创建上传器
	uploader, err := indexerClient.NewUploaderFromIndexerNodes(
		ctx,
		file.NumSegments(),
		w3client,
		1,     // expectedReplica
		nil,   // dropped nodes
		"min", // method
		true,  // fullTrusted
	)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create uploader")
	}

	// 上传选项
	uploadOpt := transfer.UploadOption{
		FinalityRequired: transfer.FileFinalized,
		TaskSize:         5,
		ExpectedReplica:  1,
		SkipTx:           true,
		Method:           "min",
		FullTrusted:      true,
	}

	// 执行上传
	logrus.Info("开始上传文件...")
	txHashes, rootHashes, err := uploader.SplitableUpload(ctx, file, fragmentSize, uploadOpt)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to upload file")
	}

	logrus.Infof("上传成功! 交易数: %d, 片段数: %d", len(txHashes), len(rootHashes))
	for i, root := range rootHashes {
		logrus.Infof("片段 %d root hash: %s", i+1, root.Hex())
	}

	// 等待一段时间确保文件完全同步
	time.Sleep(5 * time.Second)

	// 下载文件
	outputPath := "downloaded_file.bin"
	logrus.Info("开始下载文件...")

	// 将所有 root hash 转换为字符串数组
	roots := make([]string, len(rootHashes))
	for i, root := range rootHashes {
		roots[i] = root.Hex()
	}

	// 使用 DownloadFragments 下载所有片段并合并
	err = indexerClient.DownloadFragments(ctx, roots, outputPath, true)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to download file")
	}

	logrus.Infof("下载成功! 文件保存至: %s", outputPath)
}
