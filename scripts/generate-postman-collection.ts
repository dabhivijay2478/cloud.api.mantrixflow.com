/**
 * Generate Postman Collection from OpenAPI/Swagger Spec
 * This script fetches the OpenAPI JSON and converts it to Postman Collection v2.1 format
 */

import * as fs from 'node:fs';
import * as http from 'node:http';
import * as path from 'node:path';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const converter = require('openapi-to-postmanv2');

const PORT = process.env.PORT || 8000;
const API_URL = `http://localhost:${PORT}`;
const OPENAPI_JSON_URL = `${API_URL}/api/docs-json`; // Swagger JSON endpoint
const OUTPUT_DIR = path.join(__dirname, '../postman');

/**
 * Generate dynamic filename from OpenAPI spec
 */
function generateFileName(openapiSpec: OpenAPISpec): string {
  // Get API name from OpenAPI spec
  const apiName =
    openapiSpec.info?.title && typeof openapiSpec.info.title === 'string'
      ? openapiSpec.info.title
      : 'API';

  // Sanitize filename: remove special characters, replace spaces with underscores
  const sanitizedName = String(apiName)
    .replace(/[^a-zA-Z0-9\s-_]/g, '') // Remove special chars
    .replace(/\s+/g, '_') // Replace spaces with underscores
    .replace(/_+/g, '_') // Replace multiple underscores with single
    .toLowerCase();

  // Get version if available
  const version =
    openapiSpec.info?.version && typeof openapiSpec.info.version === 'string'
      ? openapiSpec.info.version
      : '1.0.0';
  const sanitizedVersion = String(version).replace(/[^a-zA-Z0-9.-]/g, '');

  // Generate timestamp for uniqueness
  const timestamp = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format

  // Generate filename: {api_name}_v{version}_{date}.postman_collection.json
  return `${sanitizedName}_v${sanitizedVersion}_${timestamp}.postman_collection.json`;
}

interface OpenAPIPath {
  [method: string]: {
    summary?: string;
    description?: string;
    parameters?: any[];
    requestBody?: any;
    responses?: any;
    tags?: string[];
  };
}

interface OpenAPISpec {
  openapi: string;
  info: any;
  servers: any[];
  paths: {
    [path: string]: OpenAPIPath;
  };
  components?: any;
}

interface PostmanCollection {
  info: {
    name: string;
    description: string;
    schema: string;
    _exporter_id: string;
  };
  item: any[];
  variable?: any[];
}

/**
 * Convert OpenAPI spec to Postman Collection using openapi-to-postmanv2
 */
function convertOpenAPIToPostman(openapi: OpenAPISpec): Promise<any> {
  return new Promise((resolve, reject) => {
    converter.convert({ type: 'json', data: openapi }, {}, (err: Error | null, result: any) => {
      if (err) {
        reject(err);
      } else {
        if (result.result && result.result.length > 0) {
          resolve(result.result[0].output[0].data);
        } else {
          reject(new Error('Failed to convert OpenAPI to Postman collection'));
        }
      }
    });
  });
}

/**
 * Convert OpenAPI spec to Postman Collection (Manual fallback)
 */
function convertOpenAPIToPostmanManual(openapi: OpenAPISpec): PostmanCollection {
  const collection: PostmanCollection = {
    info: {
      name: openapi.info.title || 'MantrixFlow PostgreSQL Connector API',
      description: openapi.info.description || '',
      schema: 'https://schema.getpostman.com/json/collection/v2.1.0/collection.json',
      _exporter_id: 'mantrixflow-postgres-connector',
    },
    item: [],
    variable: openapi.servers?.map((server, index) => ({
      key: `baseUrl${index === 0 ? '' : index}`,
      value: server.url,
      type: 'string',
    })),
  };

  // Group endpoints by tags
  const itemsByTag: { [tag: string]: any[] } = {};

  Object.entries(openapi.paths).forEach(([path, methods]) => {
    Object.entries(methods).forEach(([method, operation]) => {
      const tag = operation.tags?.[0] || 'default';
      if (!itemsByTag[tag]) {
        itemsByTag[tag] = [];
      }

      const item: any = {
        name: operation.summary || `${method.toUpperCase()} ${path}`,
        request: {
          method: method.toUpperCase(),
          header: [],
          url: {
            raw: `{{baseUrl}}${path}`,
            host: ['{{baseUrl}}'],
            path: path.split('/').filter((p) => p),
          },
          description: operation.description || '',
        },
        response: [],
      };

      // Add parameters
      if (operation.parameters) {
        item.request.url.query = operation.parameters
          .filter((p: any) => p.in === 'query')
          .map((p: any) => ({
            key: p.name,
            value: p.example || '',
            description: p.description || '',
          }));

        operation.parameters
          .filter((p: any) => p.in === 'path')
          .forEach((p: any) => {
            const pathIndex = item.request.url.path.findIndex(
              (seg: string) => seg === `:${p.name}` || seg === `{${p.name}}`,
            );
            if (pathIndex !== -1) {
              item.request.url.path[pathIndex] = `:${p.name}`;
            }
          });
      }

      // Add request body
      if (operation.requestBody) {
        const content = operation.requestBody.content;
        if (content) {
          const contentType = Object.keys(content)[0];
          const schema = content[contentType].schema;

          item.request.header.push({
            key: 'Content-Type',
            value: contentType,
          });

          if (schema?.properties) {
            item.request.body = {
              mode: 'raw',
              raw: JSON.stringify(generateExampleFromSchema(schema), null, 2),
              options: {
                raw: {
                  language: 'json',
                },
              },
            };
          }
        }
      }

      // Add example responses
      if (operation.responses) {
        Object.entries(operation.responses).forEach(([statusCode, response]: [string, any]) => {
          if (response.content?.['application/json']) {
            const schema = response.content['application/json'].schema;
            item.response.push({
              name: `${statusCode} ${response.description || ''}`,
              originalRequest: { ...item.request },
              status: statusCode,
              code: parseInt(statusCode, 10),
              _postman_previewlanguage: 'json',
              header: [
                {
                  key: 'Content-Type',
                  value: 'application/json',
                },
              ],
              body: JSON.stringify(generateExampleFromSchema(schema), null, 2),
            });
          }
        });
      }

      itemsByTag[tag].push(item);
    });
  });

  // Organize items by tags
  Object.entries(itemsByTag).forEach(([tag, items]) => {
    collection.item.push({
      name: tag,
      item: items,
    });
  });

  return collection;
}

/**
 * Generate example JSON from OpenAPI schema
 */
function generateExampleFromSchema(schema: any): any {
  if (!schema) return {};

  if (schema.example) {
    return schema.example;
  }

  if (schema.type === 'object' && schema.properties) {
    const example: any = {};
    Object.entries(schema.properties).forEach(([key, prop]: [string, any]) => {
      example[key] = generateExampleFromSchema(prop);
    });
    return example;
  }

  if (schema.type === 'array' && schema.items) {
    return [generateExampleFromSchema(schema.items)];
  }

  // Default examples based on type
  switch (schema.type) {
    case 'string':
      return schema.enum ? schema.enum[0] : 'string';
    case 'number':
    case 'integer':
      return 0;
    case 'boolean':
      return false;
    case 'array':
      return [];
    case 'object':
      return {};
    default:
      return null;
  }
}

/**
 * Fetch OpenAPI JSON from server
 */
function fetchOpenAPISpec(): Promise<OpenAPISpec> {
  return new Promise((resolve, reject) => {
    http
      .get(OPENAPI_JSON_URL, (res) => {
        let data = '';

        res.on('data', (chunk) => {
          data += chunk;
        });

        res.on('end', () => {
          try {
            const spec = JSON.parse(data) as OpenAPISpec;
            resolve(spec);
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            reject(new Error(`Failed to parse OpenAPI spec: ${errorMessage}`));
          }
        });
      })
      .on('error', (error) => {
        const errorMessage = error instanceof Error ? error.message : String(error);
        reject(new Error(`Failed to fetch OpenAPI spec: ${errorMessage}`));
      });
  });
}

/**
 * Main function
 */
async function generatePostmanCollection() {
  console.log('🔄 Generating Postman Collection...');
  console.log(`📡 Fetching OpenAPI spec from ${OPENAPI_JSON_URL}`);

  try {
    // Wait a bit for server to be ready
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const openapiSpec = await fetchOpenAPISpec();
    console.log('✅ OpenAPI spec fetched successfully');

    // Generate dynamic filename based on OpenAPI spec
    const fileName = generateFileName(openapiSpec);
    const outputFile = path.join(OUTPUT_DIR, fileName);

    // Try using the converter library first
    let collection;
    try {
      collection = await convertOpenAPIToPostman(openapiSpec);
      console.log('✅ Converted using openapi-to-postmanv2');
    } catch {
      console.warn('⚠️  Converter library failed, using manual conversion...');
      collection = convertOpenAPIToPostmanManual(openapiSpec);
    }

    // Ensure directory exists
    if (!fs.existsSync(OUTPUT_DIR)) {
      fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    }

    // Write collection file
    fs.writeFileSync(outputFile, JSON.stringify(collection, null, 2));
    console.log(`✅ Postman collection saved to: ${outputFile}`);
    console.log(`📄 Filename: ${fileName}`);
    console.log('\n📋 Import this file into Postman to test the API!');
    console.log(`   File: postman/${fileName}`);
  } catch (error) {
    console.error('❌ Error generating Postman collection:', error);
    console.log('\n💡 Make sure the server is running: bun run start:dev');
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  void generatePostmanCollection();
}

export { generatePostmanCollection };
