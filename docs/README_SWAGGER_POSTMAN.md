# Swagger & Postman Collection Setup

## ✅ Setup Complete

Swagger/OpenAPI documentation and Postman collection generation are now configured!

## Quick Start

### 1. Access Swagger UI

Start your server:
```bash
bun run start:dev
```

Then open in your browser:
```
http://localhost:8000/api/docs
```

### 2. Generate Postman Collection

While the server is running, in another terminal:
```bash
bun run swagger:generate
```

The collection will be saved to:
```
postman/MantrixFlow_PostgreSQL_Connector.postman_collection.json
```

### 3. Import into Postman

1. Open Postman
2. Click **Import** button (top left)
3. Select the file: `postman/MantrixFlow_PostgreSQL_Connector.postman_collection.json`
4. All endpoints will be available with example requests!

## What's Included

### Swagger UI Features
- ✅ Interactive API documentation
- ✅ Try-it-out functionality (test endpoints directly)
- ✅ Request/Response examples
- ✅ Schema validation
- ✅ JWT Bearer authentication support

### Postman Collection Features
- ✅ All API endpoints organized by tags
- ✅ Example request bodies
- ✅ Environment variables (baseUrl)
- ✅ Response examples
- ✅ Ready to test immediately

## API Endpoints

### Connection Management
- `POST /api/connectors/postgres/test-connection` - Test connection
- `POST /api/connectors/postgres/connections` - Create connection
- `GET /api/connectors/postgres/connections` - List connections
- `GET /api/connectors/postgres/connections/:id` - Get connection
- `PATCH /api/connectors/postgres/connections/:id` - Update connection
- `DELETE /api/connectors/postgres/connections/:id` - Delete connection

### Schema Discovery
- `GET /api/connectors/postgres/connections/:id/databases` - List databases
- `GET /api/connectors/postgres/connections/:id/schemas` - List schemas
- `GET /api/connectors/postgres/connections/:id/tables` - List tables
- `GET /api/connectors/postgres/connections/:id/tables/:table/schema` - Get table schema
- `POST /api/connectors/postgres/connections/:id/refresh-schema` - Refresh schema

### Query Execution
- `POST /api/connectors/postgres/connections/:id/query` - Execute query
- `POST /api/connectors/postgres/connections/:id/query/explain` - Explain query

### Data Synchronization
- `POST /api/connectors/postgres/connections/:id/sync` - Create sync job
- `GET /api/connectors/postgres/connections/:id/sync-jobs` - List sync jobs
- `GET /api/connectors/postgres/connections/:id/sync-jobs/:jobId` - Get sync job
- `POST /api/connectors/postgres/connections/:id/sync-jobs/:jobId/cancel` - Cancel sync

### Monitoring
- `GET /api/connectors/postgres/connections/:id/health` - Connection health
- `GET /api/connectors/postgres/connections/:id/metrics` - Connection metrics
- `GET /api/connectors/postgres/connections/:id/query-logs` - Query logs

## OpenAPI JSON

The OpenAPI specification is available at:
```
http://localhost:8000/api/docs-json
```

You can use this URL to:
- Import into Postman (Import > Link)
- Use with Swagger Editor
- Generate client SDKs
- Use with other API tools

## Testing in Postman

1. **Set Environment Variables**:
   - `baseUrl`: `http://localhost:8000`
   - `token`: Your JWT token (if using auth)

2. **Test Endpoints**:
   - All endpoints are pre-configured
   - Request bodies have example values
   - Just update the values and send!

3. **Example: Test Connection**:
   ```json
   POST {{baseUrl}}/api/connectors/postgres/test-connection
   {
     "host": "localhost",
     "port": 5432,
     "database": "mydb",
     "username": "postgres",
     "password": "password"
   }
   ```

## DTOs

All endpoints use TypeScript DTOs with:
- Validation decorators
- Swagger documentation
- Type safety

DTOs are located in:
- `src/modules/connectors/postgres/dto/`

## Authentication

Currently using placeholder authentication. To add real JWT auth:

1. Add JWT guards to controller
2. Update Swagger to show auth requirements
3. Add token to Postman collection variables

## Troubleshooting

### Swagger UI not loading
- Make sure server is running on port 8000
- Check console for errors
- Verify `@nestjs/swagger` is installed

### Postman collection generation fails
- Ensure server is running
- Check that `/api/docs-json` endpoint is accessible
- Verify `openapi-to-postmanv2` is installed

### Build errors
- Run `bun install` to ensure all dependencies are installed
- Check TypeScript compilation: `bun run build`

## Next Steps

1. ✅ Swagger UI configured
2. ✅ Postman collection generator ready
3. ⏭️ Add authentication guards
4. ⏭️ Add more detailed examples
5. ⏭️ Set up Postman environments

