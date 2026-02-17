/**
 * Connector metadata for dynamic UI form generation.
 * Maps source_type to required/optional fields and UI schema.
 * Adding a new connector = new entry here + plugin in meltano.yml.
 */

export interface ConnectorFieldSchema {
  key: string;
  label: string;
  type: 'string' | 'number' | 'boolean' | 'password' | 'object';
  required: boolean;
  placeholder?: string;
  description?: string;
  default?: unknown;
}

export interface ConnectorMetadata {
  sourceType: string;
  displayName: string;
  requiredFields: string[];
  optionalFields: string[];
  uiSchema: ConnectorFieldSchema[];
}

export const CONNECTOR_METADATA: ConnectorMetadata[] = [
  {
    sourceType: 'postgresql',
    displayName: 'PostgreSQL',
    requiredFields: ['host', 'port', 'database', 'username', 'password'],
    optionalFields: ['schema', 'ssl', 'connection_string'],
    uiSchema: [
      { key: 'host', label: 'Host', type: 'string', required: true, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: true, default: 5432 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'username', label: 'Username', type: 'string', required: true },
      { key: 'password', label: 'Password', type: 'password', required: true },
      { key: 'schema', label: 'Schema', type: 'string', required: false, default: 'public' },
      {
        key: 'connection_string',
        label: 'Connection String',
        type: 'string',
        required: false,
        description: 'Alternative to host/port/user/pass',
      },
      {
        key: 'ssl',
        label: 'Enable SSL',
        type: 'boolean',
        required: false,
        description: 'Enable SSL for secure connections',
        default: false,
      },
    ],
  },
  {
    sourceType: 'mysql',
    displayName: 'MySQL',
    requiredFields: ['host', 'port', 'database', 'username', 'password'],
    optionalFields: ['schema', 'ssl', 'connection_string'],
    uiSchema: [
      { key: 'host', label: 'Host', type: 'string', required: true, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: true, default: 3306 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'username', label: 'Username', type: 'string', required: true },
      { key: 'password', label: 'Password', type: 'password', required: true },
      { key: 'connection_string', label: 'Connection String', type: 'string', required: false },
      {
        key: 'ssl',
        label: 'Enable SSL',
        type: 'boolean',
        required: false,
        description: 'Enable SSL/TLS for secure connections',
        default: false,
      },
    ],
  },
  {
    sourceType: 'mongodb',
    displayName: 'MongoDB',
    requiredFields: ['database'],
    optionalFields: [
      'host',
      'port',
      'username',
      'password',
      'connection_string',
      'auth_source',
      'replica_set',
      'tls',
    ],
    uiSchema: [
      {
        key: 'connection_string',
        label: 'Connection String',
        type: 'string',
        required: false,
        placeholder: 'mongodb://localhost:27017',
        description: 'mongodb:// or mongodb+srv://',
      },
      { key: 'host', label: 'Host', type: 'string', required: false, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: false, default: 27017 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'username', label: 'Username', type: 'string', required: false },
      { key: 'password', label: 'Password', type: 'password', required: false },
      {
        key: 'auth_source',
        label: 'Auth Source',
        type: 'string',
        required: false,
        default: 'admin',
      },
      { key: 'replica_set', label: 'Replica Set', type: 'string', required: false },
      {
        key: 'tls',
        label: 'Enable TLS/SSL',
        type: 'boolean',
        required: false,
        description: 'Enable TLS for secure connections',
        default: false,
      },
    ],
  },
];
