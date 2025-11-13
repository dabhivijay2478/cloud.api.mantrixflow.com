/**
 * Encryption Service
 * Handles AES-256-GCM encryption/decryption for sensitive data
 * Uses crypto.scrypt for key derivation
 */

import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as crypto from 'crypto';

@Injectable()
export class EncryptionService {
  private readonly algorithm = 'aes-256-gcm';
  private readonly keyLength = 32; // 256 bits
  private readonly ivLength = 16; // 128 bits
  private readonly saltLength = 64; // 512 bits
  private readonly tagLength = 16; // 128 bits
  private readonly scryptOptions = {
    N: 16384, // CPU/memory cost parameter
    r: 8, // Block size parameter
    p: 1, // Parallelization parameter
  };

  constructor(private readonly configService: ConfigService) {}

  /**
   * Get encryption key from environment variable or derive from master key
   */
  private getEncryptionKey(): Buffer {
    const masterKey = this.configService.get<string>('ENCRYPTION_MASTER_KEY');
    if (!masterKey) {
      throw new Error('ENCRYPTION_MASTER_KEY environment variable is not set');
    }

    // Derive a consistent key from master key
    // In production, you might want to use a key derivation service
    return crypto.scryptSync(
      masterKey,
      'postgres-connector-salt',
      this.keyLength,
      this.scryptOptions,
    );
  }

  /**
   * Encrypt data using AES-256-GCM
   * Format: base64(salt:iv:tag:ciphertext)
   */
  encrypt(plaintext: string): string {
    if (!plaintext || typeof plaintext !== 'string') {
      throw new Error('Plaintext must be a non-empty string');
    }

    try {
      const key = this.getEncryptionKey();
      const salt = crypto.randomBytes(this.saltLength);
      const iv = crypto.randomBytes(this.ivLength);

      const cipher = crypto.createCipheriv(this.algorithm, key, iv);
      cipher.setAAD(Buffer.from('postgres-connector')); // Additional authenticated data

      let ciphertext = cipher.update(plaintext, 'utf8');
      ciphertext = Buffer.concat([ciphertext, cipher.final()]);

      const tag = cipher.getAuthTag();

      // Format: salt:iv:tag:ciphertext (all base64 encoded)
      const saltB64 = salt.toString('base64');
      const ivB64 = iv.toString('base64');
      const tagB64 = tag.toString('base64');
      const ciphertextB64 = ciphertext.toString('base64');

      return `${saltB64}:${ivB64}:${tagB64}:${ciphertextB64}`;
    } catch (error) {
      throw new Error(
        `Encryption failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Decrypt data using AES-256-GCM
   * Expects format: base64(salt:iv:tag:ciphertext)
   */
  decrypt(encryptedData: string): string {
    if (!encryptedData || typeof encryptedData !== 'string') {
      throw new Error('Encrypted data must be a non-empty string');
    }

    try {
      const parts = encryptedData.split(':');
      if (parts.length !== 4) {
        throw new Error(
          'Invalid encryption format. Expected format: salt:iv:tag:ciphertext',
        );
      }

      const [saltB64, ivB64, tagB64, ciphertextB64] = parts;

      const salt = Buffer.from(saltB64, 'base64');
      const iv = Buffer.from(ivB64, 'base64');
      const tag = Buffer.from(tagB64, 'base64');
      const ciphertext = Buffer.from(ciphertextB64, 'base64');

      // Validate lengths
      if (salt.length !== this.saltLength) {
        throw new Error('Invalid salt length');
      }
      if (iv.length !== this.ivLength) {
        throw new Error('Invalid IV length');
      }
      if (tag.length !== this.tagLength) {
        throw new Error('Invalid tag length');
      }

      // Note: In this implementation, we use a fixed key derived from master key
      // The salt is stored but not used for key derivation in this simple implementation
      // For production, you might want to derive the key from the salt
      const key = this.getEncryptionKey();

      const decipher = crypto.createDecipheriv(this.algorithm, key, iv);
      decipher.setAuthTag(tag);
      decipher.setAAD(Buffer.from('postgres-connector'));

      let plaintext = decipher.update(ciphertext);
      plaintext = Buffer.concat([plaintext, decipher.final()]);

      return plaintext.toString('utf8');
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.includes('Unsupported state')
      ) {
        throw new Error(
          'Decryption failed: Authentication tag mismatch. Data may be corrupted or tampered.',
        );
      }
      throw new Error(
        `Decryption failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Encrypt multiple fields at once
   */
  encryptFields(fields: Record<string, string>): Record<string, string> {
    const encrypted: Record<string, string> = {};
    for (const [key, value] of Object.entries(fields)) {
      if (value) {
        encrypted[key] = this.encrypt(value);
      }
    }
    return encrypted;
  }

  /**
   * Decrypt multiple fields at once
   */
  decryptFields(fields: Record<string, string>): Record<string, string> {
    const decrypted: Record<string, string> = {};
    for (const [key, value] of Object.entries(fields)) {
      if (value) {
        decrypted[key] = this.decrypt(value);
      }
    }
    return decrypted;
  }

  /**
   * Constant-time comparison to prevent timing attacks
   */
  private constantTimeEquals(a: Buffer, b: Buffer): boolean {
    if (a.length !== b.length) {
      return false;
    }

    let result = 0;
    for (let i = 0; i < a.length; i++) {
      result |= a[i] ^ b[i];
    }

    return result === 0;
  }
}
