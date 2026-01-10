# Dashboard Module

## Route Configuration

- **Controller Path**: `dashboard`
- **Global Prefix**: `api` (set in `main.ts`)
- **Full Route**: `/api/dashboard/overview`
- **Method**: `GET`
- **Query Parameter**: `organizationId` (required, UUID)

## Endpoint

```
GET /api/dashboard/overview?organizationId=<uuid>
```

## Response Structure

```json
{
  "data": {
    "organization": {
      "id": "string",
      "name": "string",
      "memberCount": 0,
      "createdAt": "2024-01-01T00:00:00.000Z"
    },
    "pipelines": {
      "total": 0,
      "active": 0,
      "paused": 0,
      "failed": 0,
      "byStatus": {
        "running": 0,
        "completed": 0,
        "failed": 0,
        "pending": 0
      }
    },
    "recentMigrations": [],
    "recentActivity": []
  },
  "meta": {
    "message": "Dashboard overview retrieved successfully"
  }
}
```

## Dependencies

- OrganizationModule (for organization and member data)
- DataPipelineModule (for pipeline and run data)
- ActivityLogModule (for activity log data)
- DRIZZLE_DB (database connection)

## Troubleshooting

If you get a 404 error:
1. Ensure the server has been restarted after adding this module
2. Verify DashboardModule is imported in AppModule
3. Check that the global prefix 'api' is set in main.ts
4. Verify the controller path is 'dashboard' (not 'api/dashboard')
