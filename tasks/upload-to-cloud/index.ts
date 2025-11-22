import type { Context } from "@oomol/types/oocana";
import fs from "fs";
import path from "path";

//#region generated meta
type Inputs = {
  file: string;
};
type Outputs = {
  remote_url: string;
};
//#endregion

// 预签名URL响应类型
type InitUploadResponse = {
  data: {
    uploadURL: string;
    downloadURL: string;
    fields: Record<string, string>;
  }
};

// 分段上传配置
const MAX_CONCURRENT_UPLOADS = 3; // 最大并发上传数量
const MULTIPART_THRESHOLD = 5 * 1024 * 1024; // 5MB 分段上传阈值

// 文件分片接口
interface FileChunk {
    index: number;
    start: number;
    end: number;
    size: number;
    data: ArrayBuffer;
}

// 上传进度接口
interface UploadProgress {
    uploadedBytes: number;
    totalBytes: number;
    percentage: number;
    uploadedChunks: number;
    totalChunks: number;
}

// 分段上传结果接口
interface MultipartUploadResult {
    downloadURL: string;
    uploadID: string;
    key: string;
}

// API 响应类型
interface PresignedUrlResponse {
    partNumber: number;
    uploadURL: string;
}

// 错误类型
enum UploadError {
    FILE_TOO_LARGE = "FILE_TOO_LARGE",
    NETWORK_ERROR = "NETWORK_ERROR",
    UPLOAD_FAILED = "UPLOAD_FAILED",
    INVALID_RESPONSE = "INVALID_RESPONSE",
}

// 创建分段上传响应
interface CreateMultipartUploadResponse {
    data: {
        uploadID: string;
        key: string;
        partSize: number;
    };
}

// 生成预签名URL响应
interface GeneratePresignedUrlsResponse {
    data: PresignedUrlResponse[];
}

// 完成分段上传响应
interface CompleteMultipartUploadResponse {
    data: {
        downloadURL: string;
    };
}

/**
 * 分段上传类
 */
class MultipartUploader {
    private file: File;
    private fileSuffix: string;
    private apiBaseUrl: string;
    private authToken?: string;
    private onProgress?: (progress: UploadProgress) => void;

    constructor(
        file: File,
        authToken: string,
        onProgress?: (progress: UploadProgress) => void,
    ) {
        this.file = file;
        this.fileSuffix = this.getFileSuffix(file.name);
        this.apiBaseUrl = "https://fusion-api.oomol.com";
        this.authToken = authToken;
        this.onProgress = onProgress;

        // 验证文件大小
        if (file.size > 500 * 1024 * 1024) {
            throw new Error(UploadError.FILE_TOO_LARGE);
        }
    }

    /**
     * 获取文件后缀
     */
    private getFileSuffix(fileName: string): string {
        const parts = fileName.split(".");
        return parts.length > 1 ? parts[parts.length - 1].toLowerCase() : "txt";
    }

    /**
     * 发送 API 请求
     */
    private async makeRequest(
        endpoint: string,
        body: unknown,
        method = "POST",
    ): Promise<Response> {
        const headers: Record<string, string> = {
            "Content-Type": "application/json",
        };

        if (this.authToken) {
            headers.Authorization = this.authToken;
        }

        const response = await fetch(`${this.apiBaseUrl}/v1/${endpoint}`, {
            method,
            headers,
            body: JSON.stringify(body),
        });

        return response;
    }

    /**
     * 将文件分割成指定大小的块
     */
    private async sliceFile(chunkSize: number): Promise<FileChunk[]> {
        const chunks: FileChunk[] = [];
        const totalSize = this.file.size;

        for (let i = 0; i < totalSize; i += chunkSize) {
            const start = i;
            const end = Math.min(i + chunkSize, totalSize);
            const size = end - start;

            const blob = (this.file as any).slice(start, end, this.file.type);
            const data = await blob.arrayBuffer();

            chunks.push({
                index: Math.floor(i / chunkSize) + 1, // 分片编号从 1 开始
                start,
                end,
                size,
                data,
            });
        }

        return chunks;
    }

    /**
     * 上传单个分片
     */
    private async uploadChunk(
        chunk: FileChunk,
        uploadURL: string,
    ): Promise<{ partNumber: number; etag: string }> {
        try {
            const response = await fetch(uploadURL, {
                method: "PUT",
                body: chunk.data,
                headers: {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": chunk.size.toString(),
                },
            });

            if (!response.ok) {
                throw new Error(`Upload failed with status ${response.status}`);
            }

            const etag = response.headers.get("etag");
            if (!etag) {
                throw new Error("No ETag returned from upload");
            }

            return {
                partNumber: chunk.index,
                etag: etag.replace(/"/g, ""), // 移除 ETag 周围的引号
            };
        }
        catch (error) {
            throw new Error(`Failed to upload chunk ${chunk.index}: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    /**
     * 并发上传多个分片
     */
    private async uploadChunksConcurrently(
        chunks: FileChunk[],
        presignedUrls: PresignedUrlResponse[],
    ): Promise<Array<{ partNumber: number; etag: string }>> {
        const results: Array<{ partNumber: number; etag: string }> = [];
        let uploadedBytes = 0;
        const totalBytes = this.file.size;

        // 将 presignedUrls 按 partNumber 排序，确保与 chunks 顺序一致
        const sortedUrls = presignedUrls.sort((a, b) => a.partNumber - b.partNumber);

        for (let i = 0; i < chunks.length; i += MAX_CONCURRENT_UPLOADS) {
            const batch = chunks.slice(i, i + MAX_CONCURRENT_UPLOADS);
            const urlBatch = sortedUrls.slice(i, i + MAX_CONCURRENT_UPLOADS);

            const uploadPromises = batch.map((chunk, index) =>
                this.uploadChunk(chunk, urlBatch[index].uploadURL),
            );

            try {
                const batchResults = await Promise.all(uploadPromises);
                results.push(...batchResults);

                // 更新进度
                const batchUploadedBytes = batch.reduce((sum, chunk) => sum + chunk.size, 0);
                uploadedBytes += batchUploadedBytes;

                if (this.onProgress) {
                    this.onProgress({
                        uploadedBytes,
                        totalBytes,
                        percentage: Math.round((uploadedBytes / totalBytes) * 100),
                        uploadedChunks: results.length,
                        totalChunks: chunks.length,
                    });
                }
            }
            catch (error) {
                throw new Error(`Batch upload failed: ${error instanceof Error ? error.message : String(error)}`);
            }
        }

        return results;
    }

    /**
     * 执行分段上传
     */
    async upload(): Promise<MultipartUploadResult> {
        try {
            // 1. 创建分段上传
            const createResponse = await this.makeRequest("file-upload/action/create-multipart-upload", {
                fileSuffix: this.fileSuffix,
                fileSize: this.file.size,
            });

            if (!createResponse.ok) {
                const errorData = await createResponse.json().catch(() => ({})) as any;
                throw new Error(`Failed to create multipart upload: ${createResponse.status} - ${errorData.error || "Unknown error"}`);
            }

            const createResponseData: CreateMultipartUploadResponse = await createResponse.json();
            const createResult = createResponseData.data;

            // 2. 分割文件
            const chunks = await this.sliceFile(createResult.partSize);

            // 3. 生成预签名 URL
            const partNumbers = chunks.map(chunk => chunk.index);
            const urlsResponse = await this.makeRequest("file-upload/action/generate-presigned-urls", {
                uploadID: createResult.uploadID,
                key: createResult.key,
                partNumbers,
            });

            if (!urlsResponse.ok) {
                const errorData = await urlsResponse.json().catch(() => ({})) as any;
                throw new Error(`Failed to generate presigned URLs: ${urlsResponse.status} - ${errorData.error || "Unknown error"}`);
            }

            const presignedUrlsResponse: GeneratePresignedUrlsResponse = await urlsResponse.json();
            const presignedUrls = presignedUrlsResponse.data;

            // 4. 上传分片
            const uploadedParts = await this.uploadChunksConcurrently(chunks, presignedUrls);

            // 5. 完成上传
            const completeResponse = await this.makeRequest("file-upload/action/complete-multipart-upload", {
                uploadID: createResult.uploadID,
                key: createResult.key,
                parts: uploadedParts,
            });

            if (!completeResponse.ok) {
                throw new Error(`Failed to complete upload: ${completeResponse.status}`);
            }

            const completeResponseData: CompleteMultipartUploadResponse = await completeResponse.json();
            const completeResult = completeResponseData.data;

            return {
                downloadURL: completeResult.downloadURL,
                uploadID: createResult.uploadID,
                key: createResult.key,
            };
        }
        catch (error) {
            throw new Error(`Multipart upload failed: ${error instanceof Error ? error.message : String(error)}`);
        }
    }
}

// 单文件上传函数
async function uploadFile(
  fileData: Buffer,
  filename: string,
  uploadUrl: string,
  fields: Record<string, string>,
  onProgress: (progress: number) => void,
  retries = 3
): Promise<void> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      // 创建FormData用于POST表单上传
      const formData = new FormData();

      // 添加所有签名字段
      Object.entries(fields).forEach(([key, value]) => {
        formData.append(key, value);
      });

      // 创建文件blob，强制设置为application/octet-stream
      const fileBlob = new Blob([new Uint8Array(fileData)], { type: "application/octet-stream" });

      // 添加文件到FormData
      formData.append("file", fileBlob, filename);

      const response = await fetch(uploadUrl, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Upload failed with status: ${response.status} - ${errorText}`);
      }

      // 上传成功，报告100%进度
      onProgress(100);
      return;
    } catch (error) {
      if (attempt === retries) {
        throw new Error(
          `Failed to upload file after ${retries} attempts: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
      }
      // 等待一段时间后重试
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
    }
  }
}

/**
 * 检查文件是否应该使用分段上传
 */
function shouldUseMultipartUpload(fileSize: number): boolean {
    // 大于 5MB 的文件使用分段上传
    return fileSize > MULTIPART_THRESHOLD;
}

/**
 * 使用分段上传上传文件
 */
async function uploadFileMultipart(
    filePath: string,
    authToken: string,
    onProgress: (progress: number) => void,
): Promise<string> {
    const fileBuffer = fs.readFileSync(filePath);
    const filename = path.basename(filePath);

    // 创建 File 对象
    const file = new File([fileBuffer], filename, { type: "application/octet-stream" });

    const uploader = new MultipartUploader(
        file,
        authToken,
        (progress: UploadProgress) => {
            if (progress.percentage === 100) {
                // 全部上传完成不报 100 , 最后才报
                onProgress(99);
            } else {
                onProgress(progress.percentage);
            }
        }
    );

    const result = await uploader.upload();
    return result.downloadURL;
}

export default async function (
  params: Inputs,
  context: Context<Inputs, Outputs>
): Promise<Outputs> {
  const filePath = params.file;

  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  // 获取文件信息
  const fileStats = fs.statSync(filePath);
  const fileSize = fileStats.size;
  const fileExtension = path.extname(filePath);

  // Get API key from environment
  const apiKey = await context.getOomolToken();

  if (!apiKey) {
    throw new Error("OOMOL_API_KEY environment variable is not set");
  }

  console.log(`File: ${path.basename(filePath)}, Size: ${(fileSize / 1024 / 1024).toFixed(2)}MB, Threshold: ${MULTIPART_THRESHOLD / 1024 / 1024}MB`);

  // 进度报告函数
  const updateProgress = (progress: number) => {
    if (progress === 100) {
      // 全部上传完成不报 100 , 最后才报
      context.reportProgress(99);
    } else {
      context.reportProgress(progress);
    }
  };

  try {
    // 根据文件大小选择上传方式
    if (shouldUseMultipartUpload(fileSize)) {
      console.log("Using multipart upload for large file");
      // 大文件使用分段上传
      const downloadURL = await uploadFileMultipart(filePath, apiKey, updateProgress);
      context.reportProgress(100);
      return {
        remote_url: downloadURL,
      };
    } else {
      console.log("Using single file upload for small file");
      // 小文件使用原有的单文件上传
      // Step 1: 获取预签名上传URL
      const initResponse = await fetch(
        "https://fusion-api.oomol.com/v1/file-upload/action/generate-presigned-url",
        {
          method: "POST",
          headers: {
            "Authorization": apiKey,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            fileSuffix: fileExtension.replace(".", ""),
          }),
        }
      );

      if (!initResponse.ok) {
        throw new Error(`Failed to init upload: ${initResponse.status} ${initResponse.statusText}`);
      }

      const initData: InitUploadResponse = await initResponse.json();
      const { uploadURL, downloadURL, fields } = initData.data;

      if (!uploadURL || !downloadURL || !fields) {
        throw new Error("Invalid API response: missing uploadURL, downloadURL, or fields");
      }

      console.log("upload url:", uploadURL);

      // Step 2: 读取文件并上传到云存储
      const fileBuffer = fs.readFileSync(filePath);
      const filename = path.basename(filePath);

      // 上传文件
      await uploadFile(fileBuffer, filename, uploadURL, fields, updateProgress);

      context.reportProgress(100);

      return {
        remote_url: downloadURL,
      };
    }
  } catch (error) {
    throw new Error(`Upload failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}