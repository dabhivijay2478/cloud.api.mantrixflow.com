/**
 * Cursor Encoding/Decoding Utility
 * 
 * Provides explicit cursor encoding/decoding for cursor-based pagination.
 * Cursor format: Base64-encoded JSON containing createdAt (ISO string) and id (UUID).
 * 
 * This ensures:
 * - No Date objects are passed to postgres-js
 * - Cursor format is stable and explicit
 * - Cursor can be validated and decoded safely
 */

export interface ActivityLogCursor {
  createdAt: string; // ISO 8601 timestamp string (never Date object)
  id: string; // UUID string
}

/**
 * Encode cursor to Base64 string
 * 
 * @param cursor Cursor object with createdAt (ISO string) and id
 * @returns Base64-encoded cursor string
 */
export function encodeCursor(cursor: ActivityLogCursor): string {
  if (!cursor.createdAt || !cursor.id) {
    throw new Error('Cursor must have both createdAt and id');
  }
  
  // Validate createdAt is a valid ISO string
  if (typeof cursor.createdAt !== 'string') {
    throw new Error('Cursor createdAt must be an ISO string, not a Date object');
  }
  
  // Validate id is a string
  if (typeof cursor.id !== 'string') {
    throw new Error('Cursor id must be a string');
  }
  
  const payload = JSON.stringify({
    createdAt: cursor.createdAt,
    id: cursor.id,
  });
  
  return Buffer.from(payload).toString('base64url');
}

/**
 * Decode cursor from Base64 string
 * 
 * @param cursorString Base64-encoded cursor string
 * @returns Decoded cursor object
 * @throws Error if cursor is invalid or malformed
 */
export function decodeCursor(cursorString: string): ActivityLogCursor {
  if (!cursorString || typeof cursorString !== 'string') {
    throw new Error('Cursor must be a non-empty string');
  }
  
  try {
    const payload = Buffer.from(cursorString, 'base64url').toString('utf-8');
    const cursor = JSON.parse(payload) as ActivityLogCursor;
    
    // Validate decoded cursor structure
    if (!cursor.createdAt || typeof cursor.createdAt !== 'string') {
      throw new Error('Decoded cursor must have createdAt as ISO string');
    }
    
    if (!cursor.id || typeof cursor.id !== 'string') {
      throw new Error('Decoded cursor must have id as string');
    }
    
    // Validate createdAt is a valid ISO timestamp
    const date = new Date(cursor.createdAt);
    if (isNaN(date.getTime())) {
      throw new Error('Cursor createdAt must be a valid ISO timestamp');
    }
    
    // Validate id is a valid UUID format (basic check)
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(cursor.id)) {
      throw new Error('Cursor id must be a valid UUID');
    }
    
    return {
      createdAt: cursor.createdAt,
      id: cursor.id,
    };
  } catch (error) {
    if (error instanceof Error) {
      throw new Error(`Invalid cursor format: ${error.message}`);
    }
    throw new Error('Invalid cursor format: failed to decode');
  }
}

/**
 * Create cursor from activity log
 * 
 * @param log Activity log with createdAt and id
 * @returns Encoded cursor string
 */
export function createCursorFromLog(log: { createdAt: Date | string; id: string }): string {
  // Convert createdAt to ISO string if it's a Date object
  const createdAtStr = log.createdAt instanceof Date 
    ? log.createdAt.toISOString()
    : typeof log.createdAt === 'string'
      ? log.createdAt
      : new Date(log.createdAt).toISOString();
  
  return encodeCursor({
    createdAt: createdAtStr,
    id: log.id,
  });
}
