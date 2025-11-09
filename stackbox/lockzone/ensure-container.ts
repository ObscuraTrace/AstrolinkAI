import type { ContainerClient, BlobServiceClient } from "@azure/storage-blob"
import { getContainerClient } from "./blobClient"

/**
 * Ensures the container exists and optionally verifies access level and metadata
 */
export async function ensureContainer(
  connectionString: string,
  containerName: string
): Promise<ContainerClient> {
  const container = getContainerClient(connectionString, containerName)
  const exists = await container.exists()

  if (!exists) {
    console.log(`Container '${containerName}' not found, creating...`)
    await container.create({
      access: "container",
      metadata: {
        created: new Date().toISOString(),
        createdBy: "AstrolinkAI"
      }
    })
    console.log(`Container '${containerName}' created successfully`)
  } else {
    console.log(`Container '${containerName}' already exists`)
  }

  try {
    const properties = await container.getProperties()
    console.log(`Container last modified: ${properties.lastModified}`)
  } catch (error) {
    console.error("Failed to fetch container properties:", error)
  }

  return container
}

/**
 * Lists all blobs within a container for diagnostic or synchronization purposes
 */
export async function listContainerBlobs(
  connectionString: string,
  containerName: string
): Promise<void> {
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString)
  const containerClient = blobServiceClient.getContainerClient(containerName)

  console.log(`Listing blobs in container '${containerName}':`)
  for await (const blob of containerClient.listBlobsFlat()) {
    console.log(`- ${blob.name} (${blob.properties.contentLength || 0} bytes)`)
  }
}

/**
 * Deletes a container after confirmation
 */
export async function deleteContainerIfConfirmed(
  connectionString: string,
  containerName: string,
  confirm: boolean
): Promise<void> {
  if (!confirm) {
    console.log(`Deletion of '${containerName}' canceled`)
    return
  }

  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString)
  const containerClient = blobServiceClient.getContainerClient(containerName)

  const exists = await containerClient.exists()
  if (!exists) {
    console.warn(`Container '${containerName}' does not exist`)
    return
  }

  console.log(`Deleting container '${containerName}'...`)
  await containerClient.delete()
  console.log(`Container '${containerName}' deleted successfully`)
}

/**
 * Uploads a file buffer into the container for quick testing
 */
export async function uploadBufferToContainer(
  connectionString: string,
  containerName: string,
  blobName: string,
  buffer: Buffer
): Promise<void> {
  const container = await ensureContainer(connectionString, containerName)
  const blockBlobClient = container.getBlockBlobClient(blobName)

  console.log(`Uploading blob '${blobName}' (${buffer.length} bytes)`)
  await blockBlobClient.uploadData(buffer, {
    blobHTTPHeaders: { blobContentType: "application/octet-stream" }
  })
  console.log(`Blob '${blobName}' uploaded successfully`)
}
