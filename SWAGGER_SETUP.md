# Swagger/OpenAPI Documentation Setup

## ✅ Setup Complete

Swagger/OpenAPI documentation has been successfully configured for the PostgreSQL Connector API.

## Accessing Swagger UI

Once the server is running, access the Swagger documentation at:

```
http://localhost:8000/api/docs
```

## Features

- **Interactive API Documentation**: Browse and test all endpoints directly from the browser
- **Request/Response Examples**: See example payloads for all endpoints
- **Authentication Support**: JWT Bearer token authentication configured
- **Schema Validation**: All DTOs are documented with validation rules

## API Endpoints Documented

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
- `POST /api/connectors/postgres/connections/:id/refresh-schema` - Refresh schema cache

### Query Execution
- `POST /api/connectors/postgres/connections/:id/query` - Execute query
- `POST /api/connectors/postgres/connections/:id/query/explain` - Explain query

### Data Synchronization
- `POST /api/connectors/postgres/connections/:id/sync` - Create sync job
- `GET /api/connectors/postgres/connections/:id/sync-jobs` - List sync jobs
- `GET /api/connectors/postgres/connections/:id/sync-jobs/:jobId` - Get sync job
- `POST /api/connectors/postgres/connections/:id/sync-jobs/:jobId/cancel` - Cancel sync job

### Monitoring
- `GET /api/connectors/postgres/connections/:id/health` - Connection health
- `GET /api/connectors/postgres/connections/:id/metrics` - Connection metrics
- `GET /api/connectors/postgres/connections/:id/query-logs` - Query logs

## Generating Postman Collection

To generate a Postman collection for testing:

1. Start the development server:
   ```bash
   pnpm start:dev
   ```

2. In another terminal, generate the Postman collection:
   ```bash
   pnpm swagger:generate
   ```

3. The collection will be saved to:
   ```
   postman/MantrixFlow_PostgreSQL_Connector.postman_collection.json
   ```

4. Import this file into Postman:
   - Open Postman
   - Click "Import" button
   - Select the generated JSON file
   - All endpoints will be available with example requests

## OpenAPI JSON

The OpenAPI specification is available at:
```
http://localhost:8000/api/docs-json
```

You can use this URL with:
- Postman (import from URL)
- Swagger Editor
- Other API testing tools

## DTOs

All request/response DTOs are documented with:
- Field descriptions
- Validation rules
- Example values
- Type information

DTOs are located in:
- `src/modules/connectors/postgres/dto/`

## Authentication

The API uses JWT Bearer token authentication. To test authenticated endpoints:

1. Get a JWT token from your authentication service
2. In Swagger UI, click "Authorize" button
3. Enter: `Bearer <your-token>`
4. All requests will include the Authorization header

## Next Steps

1. ✅ Swagger UI is configured
2. ✅ DTOs are documented
3. ✅ Postman collection generator is ready
4. ⏭️ Add more detailed examples to endpoints
5. ⏭️ Add authentication guards (currently using placeholders)

