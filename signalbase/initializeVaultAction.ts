import { z } from "zod"
import type { Wallet } from "@coinbase/coinbase-sdk"
import type { ActionResult } from "./vaultActionHandler"
import { VaultOperationsEngine } from "./vaultOperationsEngine"


const BASE58_REGEX = /^[1-9A-HJ-NP-Za-km-z]+$/

/**
 * Schema for initializing a vault
 * - vaultId must be a UUID
 * - owner must look like a Base58 public key and be reasonably long
 * - collateral must be a finite non-negative number
 */
export const InitVaultSchema = z.object({
  vaultId: z.string().uuid(),
  owner: z
    .string()
    .min(32, "owner must be at least 32 characters")
    .regex(BASE58_REGEX, "owner must be Base58"),
  collateral: z
    .union([z.number(), z.string()])
    .transform((v) => (typeof v === "string" ? Number(v) : v))
    .refine((n) => Number.isFinite(n), "collateral must be a finite number")
    .refine((n) => n >= 0, "collateral must be non-negative"),
})

export type InitVaultInput = z.infer<typeof InitVaultSchema>

/**
 * Timeout helper to prevent indefinite hangs
 */
async function withTimeout<T>(p: Promise<T>, ms: number, tag = "initializeVault"): Promise<T> {
  let timeoutId: NodeJS.Timeout
  const timeout = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(`${tag} timed out after ${ms}ms`)), ms)
  })
  try {
    return await Promise.race([p, timeout])
  } finally {
    clearTimeout(timeoutId!)
  }
}

/**
