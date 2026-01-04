/**
 * PostgreSQL Connection String Parser Utility
 * Parses PostgreSQL connection strings (URI format) into individual components
 */

export interface ParsedConnectionString {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  ssl?: {
    enabled: boolean;
    rejectUnauthorized?: boolean;
  };
  options?: string; // For connection options like Neon endpoint ID
}

/**
 * Parse PostgreSQL connection string
 * Supports formats:
 * - postgresql://user:password@host:port/database?sslmode=require
 * - postgres://user:password@host:port/database
 * - postgresql://user:password@host/database
 */
export function parsePostgresConnectionString(connectionString: string): ParsedConnectionString {
  if (!connectionString || typeof connectionString !== 'string') {
    throw new Error('Connection string must be a non-empty string');
  }

  // Remove whitespace
  const trimmed = connectionString.trim();

  // Check if it's a URI format
  if (!trimmed.startsWith('postgresql://') && !trimmed.startsWith('postgres://')) {
    throw new Error('Connection string must start with postgresql:// or postgres://');
  }

  try {
    const url = new URL(trimmed);

    // Extract components
    const host = url.hostname || 'localhost';
    const port = url.port ? parseInt(url.port, 10) : 5432;
    const database = url.pathname?.slice(1) || ''; // Remove leading slash
    const username = decodeURIComponent(url.username || '');
    const password = decodeURIComponent(url.password || '');

    // Validate required fields
    if (!host) {
      throw new Error('Host is required in connection string');
    }
    if (!database) {
      throw new Error('Database name is required in connection string');
    }
    if (!username) {
      throw new Error('Username is required in connection string');
    }

    // Parse SSL mode from query parameters
    const sslMode = url.searchParams.get('sslmode')?.toLowerCase();
    let sslEnabled = false;
    let rejectUnauthorized = true;

    if (sslMode) {
      switch (sslMode) {
        case 'require':
        case 'prefer':
        case 'verify-ca':
        case 'verify-full':
          sslEnabled = true;
          if (sslMode === 'verify-ca' || sslMode === 'verify-full') {
            rejectUnauthorized = true;
          } else {
            rejectUnauthorized = false;
          }
          break;
        case 'disable':
        case 'allow':
          sslEnabled = false;
          break;
        default:
          sslEnabled = sslMode === 'require' || sslMode === 'prefer';
      }
    }

    // Check for SSL-related query parameters
    if (url.searchParams.has('ssl') && url.searchParams.get('ssl') === 'true') {
      sslEnabled = true;
    }

    // Detect Neon databases and extract endpoint ID
    // Neon hostnames follow pattern: ep-<endpoint-id>-pooler.region.aws.neon.tech
    // or ep-<endpoint-id>.region.aws.neon.tech
    let connectionOptions: string | undefined;
    const isNeonDatabase = host.includes('.neon.tech');

    if (isNeonDatabase) {
      // Extract endpoint ID (first part of hostname before first dot)
      const endpointId = host.split('.')[0];

      // Check if options parameter already exists in connection string
      const existingOptions = url.searchParams.get('options');

      if (existingOptions) {
        // Decode existing options to check if endpoint is already specified
        const decodedOptions = decodeURIComponent(existingOptions);
        if (!decodedOptions.includes('endpoint=')) {
          // Add endpoint to existing options (format: endpoint%3D<id> or endpoint=<id>)
          connectionOptions = `${existingOptions}&endpoint%3D${encodeURIComponent(endpointId)}`;
        } else {
          connectionOptions = existingOptions;
        }
      } else {
        // Add endpoint ID as connection option (URL-encoded format for connection string)
        connectionOptions = `endpoint%3D${encodeURIComponent(endpointId)}`;
      }
    } else {
      // Preserve existing options parameter if present
      const existingOptions = url.searchParams.get('options');
      if (existingOptions) {
        connectionOptions = existingOptions;
      }
    }

    return {
      host,
      port,
      database,
      username,
      password,
      ssl: sslEnabled
        ? {
            enabled: true,
            rejectUnauthorized,
          }
        : undefined,
      options: connectionOptions,
    };
  } catch (error) {
    if (error instanceof Error) {
      throw new Error(`Failed to parse connection string: ${error.message}`);
    }
    throw new Error('Failed to parse connection string: Invalid format');
  }
}
