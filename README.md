# MantrixFlow API - Backend Service

A robust NestJS-based REST API for managing data sources, data pipelines, users, organizations, and business intelligence operations. Built with TypeScript, PostgreSQL, Drizzle ORM, and Redis for job queuing.

## 🚀 Overview

MantrixFlow API is the backend service powering the MantrixFlow Business Intelligence platform. It provides comprehensive APIs for:

- **Data Source Management**: Connect and manage PostgreSQL data sources
- **Data Pipelines**: Create, manage, and execute data transformation pipelines
- **User Management**: Sync and manage users from Supabase Auth
- **Organization Management**: Multi-tenant organization and team management
- **Onboarding**: Guided onboarding flow for new users
- **Schema Discovery**: Automatic database schema discovery and mapping
- **Query Execution**: Secure query execution with audit logging
- **Data Synchronization**: Automated data sync jobs with scheduling

## ✨ Key Features

### 🔌 Data Source Management
- **PostgreSQL Connections**: Secure connection management with encrypted credentials
- **Connection Testing**: Validate connections before saving
- **Schema Discovery**: Automatic discovery of tables, columns, and relationships
- **Query Execution**: Execute custom queries with audit logging
- **Connection Pooling**: Efficient connection pool management
- **Health Monitoring**: Real-time connection health checks

### 📊 Data Pipelines
- **Pipeline Creation**: Define source-to-destination data pipelines
- **Schema Mapping**: Automatic schema transformation and mapping
- **Pipeline Execution**: Queue-based pipeline execution with BullMQ
- **Run Tracking**: Monitor pipeline runs with detailed status and logs
- **Error Handling**: Comprehensive error tracking and reporting
- **Scheduled Jobs**: Automated pipeline execution with configurable schedules

### 👥 User & Organization Management
- **User Sync**: Automatic user synchronization from Supabase Auth
- **Organization Management**: Create and manage organizations
- **Team Management**: Invite and manage team members
- **Role-Based Access**: Organization-level access control
- **Onboarding Flow**: Track user onboarding progress

### 🔒 Security
- **Supabase Auth Integration**: JWT-based authentication
- **Encrypted Credentials**: Secure storage of database credentials
- **Query Sanitization**: Protection against SQL injection
- **Audit Logging**: Complete audit trail for all operations
- **CORS Configuration**: Secure cross-origin resource sharing

## 🛠️ Tech Stack

### Core Framework
- **NestJS 11** - Progressive Node.js framework
- **TypeScript 5** - Type-safe development
- **Bun** - Fast JavaScript runtime and package manager

### Database & ORM
- **PostgreSQL** - Primary database
- **Drizzle ORM** - Type-safe SQL ORM
- **Drizzle Kit** - Database migrations and schema management

### Job Queue & Caching
- **BullMQ** - Redis-based job queue
- **Redis** - Job queue backend and caching

### Authentication
- **Supabase** - Authentication and user management
- **JWT** - Token-based authentication

### API Documentation
- **Swagger/OpenAPI** - Interactive API documentation
- **NestJS Swagger** - Automatic API documentation generation

### Development Tools
- **Biome** - Fast linter and formatter
- **Jest** - Testing framework
- **TypeScript** - Static type checking

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Bun** 1.0 or higher ([Install Bun](https://bun.sh))
- **PostgreSQL** 14+ (for the application database)
- **Redis** 6+ (for job queue)
- **Supabase Account** (for authentication)
- **Git** for version control

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd apps/api
```

### 2. Install Dependencies

```bash
bun install
```

### 3. Environment Variables Setup

Create a `.env` file in the `apps/api` directory:

```env
# Server Configuration
PORT=8000
NODE_ENV=development

# Database Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/mantrixflow
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=mantrixflow

# Supabase Configuration
SUPABASE_URL=your_supabase_project_url
SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
SUPABASE_WEBHOOK_SECRET=your_webhook_secret

# Redis Configuration (for BullMQ)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_redis_password

# Frontend URL (for CORS)
FRONTEND_URL=http://localhost:3000
NEXT_PUBLIC_APP_URL=http://localhost:3000

# Encryption Key (for encrypting database credentials)
ENCRYPTION_KEY=your_32_character_encryption_key
```

### 4. Database Setup

#### Create Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE mantrixflow;

# Exit
\q
```

#### Run Migrations

```bash
# Generate migrations from schema changes
bun run db:generate

# Run migrations
bun run db:migrate

# Or push schema directly (development only)
bun run db:push
```

### 5. Redis Setup

```bash
# Install Redis (macOS)
brew install redis

# Start Redis
redis-server

# Or using Docker
docker run -d -p 6379:6379 redis:latest
```

### 6. Run Development Server

```bash
# Development mode with hot reload
bun run start:dev

# Or production mode
bun run start:prod
```

The API will be available at:
- **API**: http://localhost:8000
- **Swagger Docs**: http://localhost:8000/api/docs

## 📁 Project Structure

```
apps/api/
├── src/
│   ├── main.ts                    # Application entry point
│   ├── app.module.ts              # Root module
│   ├── app.controller.ts          # Root controller
│   ├── app.service.ts             # Root service
│   │
│   ├── common/                    # Shared utilities
│   │   ├── dto/                   # Common DTOs
│   │   ├── encryption/            # Encryption utilities
│   │   └── guards/                # Auth guards
│   │
│   ├── database/                  # Database configuration
│   │   ├── drizzle/               # Drizzle configuration & migrations
│   │   └── schemas/               # Database schemas
│   │       ├── data-sources/      # Data source schemas
│   │       │   ├── connections/
│   │       │   ├── query-logs/
│   │       │   └── sync-jobs/
│   │       └── data-pipelines/    # Pipeline schemas
│   │           ├── pipelines/
│   │           └── pipeline-runs/
│   │
│   ├── modules/                   # Feature modules
│   │   ├── data-sources/          # Data source management
│   │   │   └── postgres/          # PostgreSQL connector
│   │   │       ├── dto/           # Data Transfer Objects
│   │   │       ├── repositories/  # Data access layer
│   │   │       ├── services/      # Business logic
│   │   │       └── utils/         # Utilities
│   │   │
│   │   ├── data-pipelines/        # Data pipeline management
│   │   │   ├── collectors/        # Data collectors
│   │   │   ├── transformers/      # Data transformers
│   │   │   ├── emitters/          # Data emitters
│   │   │   ├── repositories/      # Data access
│   │   │   └── shared/            # Shared pipeline code
│   │   │
│   │   ├── users/                 # User management
│   │   │   ├── dto/
│   │   │   ├── repositories/
│   │   │   └── webhooks/          # Supabase webhooks
│   │   │
│   │   ├── organizations/         # Organization management
│   │   │   ├── dto/
│   │   │   └── repositories/
│   │   │
│   │   └── onboarding/           # Onboarding flow
│   │
│   └── types/                     # TypeScript type definitions
│
├── test/                          # E2E tests
├── docs/                          # Documentation
├── postman/                       # Postman collections
├── scripts/                       # Utility scripts
├── drizzle.config.ts              # Drizzle configuration
├── tsconfig.json                  # TypeScript configuration
├── biome.json                     # Biome linter/formatter config
└── package.json                   # Dependencies
```

## 📜 Available Scripts

```bash
# Development
bun run start:dev      # Start development server with hot reload
bun run start:debug    # Start with debug mode
bun run start          # Start production server
bun run start:prod     # Start production server (compiled)

# Building
bun run build          # Build for production

# Database
bun run db:generate    # Generate migrations from schema
bun run db:migrate     # Run migrations
bun run db:push        # Push schema directly (dev only)
bun run db:studio      # Open Drizzle Studio
bun run db:check       # Check schema differences

# Code Quality
bun run check          # Format and lint (with auto-fix)
bun run check:ci       # Check formatting and linting (CI mode)
bun run format         # Format code
bun run format:check   # Check formatting
bun run lint           # Lint code (with auto-fix)
bun run lint:check     # Check linting

# Testing
bun run test           # Run unit tests
bun run test:watch     # Run tests in watch mode
bun run test:cov       # Run tests with coverage
bun run test:e2e       # Run E2E tests

# Documentation
bun run swagger:generate  # Generate Postman collection from Swagger
```

## 🔌 API Endpoints

### Data Sources (PostgreSQL)

#### Connections
- `POST /api/postgres/connections` - Create a new connection
- `GET /api/postgres/connections` - List all connections
- `GET /api/postgres/connections/:id` - Get connection by ID
- `PATCH /api/postgres/connections/:id` - Update connection
- `DELETE /api/postgres/connections/:id` - Delete connection
- `POST /api/postgres/connections/:id/test` - Test connection

#### Schema Discovery
- `GET /api/postgres/connections/:id/schemas` - List schemas
- `GET /api/postgres/connections/:id/tables` - List tables
- `GET /api/postgres/connections/:id/tables/:tableName` - Get table details
- `GET /api/postgres/connections/:id/columns` - List columns

#### Query Execution
- `POST /api/postgres/connections/:id/query` - Execute query
- `GET /api/postgres/connections/:id/query-logs` - Get query logs

#### Sync Jobs
- `POST /api/postgres/connections/:id/sync-jobs` - Create sync job
- `GET /api/postgres/connections/:id/sync-jobs` - List sync jobs
- `GET /api/postgres/sync-jobs/:id` - Get sync job details
- `PATCH /api/postgres/sync-jobs/:id` - Update sync job
- `DELETE /api/postgres/sync-jobs/:id` - Delete sync job

### Data Pipelines

- `POST /api/data-pipelines` - Create pipeline
- `GET /api/data-pipelines` - List pipelines
- `GET /api/data-pipelines/:id` - Get pipeline details
- `PATCH /api/data-pipelines/:id` - Update pipeline
- `DELETE /api/data-pipelines/:id` - Delete pipeline
- `POST /api/data-pipelines/:id/run` - Execute pipeline
- `GET /api/data-pipelines/:id/runs` - Get pipeline runs
- `GET /api/data-pipelines/:id/runs/:runId` - Get run details

### Users

- `POST /api/users/sync` - Sync user from Supabase
- `GET /api/users/me` - Get current user
- `GET /api/users/:id` - Get user by ID
- `PATCH /api/users/me` - Update current user
- `PATCH /api/users/me/onboarding` - Update onboarding status

### Organizations

- `POST /api/organizations` - Create organization
- `GET /api/organizations` - List organizations
- `GET /api/organizations/:id` - Get organization details
- `PATCH /api/organizations/:id` - Update organization
- `DELETE /api/organizations/:id` - Delete organization
- `POST /api/organizations/:id/members` - Invite member
- `GET /api/organizations/:id/members` - List members
- `PATCH /api/organizations/:id/members/:memberId` - Update member
- `DELETE /api/organizations/:id/members/:memberId` - Remove member

### Webhooks

- `POST /api/webhooks/supabase/user` - Supabase user webhook

## 🔐 Authentication

All API endpoints (except webhooks) require authentication via JWT token from Supabase.

### Using the API

```bash
# Get token from Supabase (frontend)
# Then use in requests:

curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
     http://localhost:8000/api/users/me
```

### Swagger UI

Interactive API documentation is available at `/api/docs` where you can:
- View all endpoints
- Test API calls
- See request/response schemas
- Authenticate with JWT token

## 🗄️ Database Schema

### Data Sources

#### `postgres_connections`
Stores PostgreSQL connection configurations with encrypted credentials.

#### `postgres_query_logs`
Audit log for all queries executed against PostgreSQL connections.

#### `postgres_sync_jobs`
Tracks data synchronization jobs from PostgreSQL sources.

### Data Pipelines

#### `postgres_pipelines`
Stores pipeline configurations with source and destination settings.

#### `postgres_pipeline_runs`
Tracks execution runs of pipelines with status and logs.

### Users & Organizations

#### `users`
User information synced from Supabase Auth.

#### `organizations`
Organization/workspace information.

#### `organization_members`
Team members and their roles within organizations.

## 🔄 Data Pipeline Architecture

### Pipeline Flow

1. **Collector**: Fetches data from source (PostgreSQL)
2. **Transformer**: Transforms data according to mapping rules
3. **Emitter**: Writes transformed data to destination
4. **Queue**: Uses BullMQ for async job processing
5. **Tracking**: Logs all runs with status and errors

### Pipeline Execution

Pipelines are executed asynchronously using BullMQ:
- Jobs are queued in Redis
- Workers process jobs in the background
- Status is tracked in `postgres_pipeline_runs` table
- Errors are logged for debugging

## 🧪 Testing

```bash
# Run unit tests
bun run test

# Run E2E tests
bun run test:e2e

# Run with coverage
bun run test:cov
```

## 📚 Documentation

Comprehensive documentation is available in the `/docs` directory:

- **[DRIZZLE_MIGRATIONS.md](./docs/DRIZZLE_MIGRATIONS.md)** - Database migration guide
- **[POSTGRES_CONNECTOR_IMPLEMENTATION.md](./docs/POSTGRES_CONNECTOR_IMPLEMENTATION.md)** - PostgreSQL connector details
- **[PIPELINE_ARCHITECTURE_REFACTOR.md](./docs/PIPELINE_ARCHITECTURE_REFACTOR.md)** - Pipeline architecture
- **[SWAGGER_SETUP.md](./docs/SWAGGER_SETUP.md)** - API documentation setup
- **[SYNC_JOBS_VS_PIPELINE_RUNS.md](./docs/SYNC_JOBS_VS_PIPELINE_RUNS.md)** - Sync jobs vs pipeline runs

## 🚢 Deployment

### Build for Production

```bash
bun run build
bun run start:prod
```

### Environment Variables for Production

Update your `.env` with production values:
- Use production database URL
- Set secure encryption keys
- Configure production Redis
- Set correct CORS origins
- Use production Supabase credentials

### Docker Deployment

```dockerfile
FROM oven/bun:latest

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --production

COPY . .
RUN bun run build

EXPOSE 8000

CMD ["bun", "run", "start:prod"]
```

### Vercel Deployment

See **[VERCEL_DEPLOYMENT.md](../../VERCEL_DEPLOYMENT.md)** at repo root for the full guide.

- **Root Directory**: `apps/api`
- **Framework**: NestJS (auto-detected)
- **Build**: `bun run build:deploy`
- **Required env**: `DATABASE_URL`, `ETL_PYTHON_SERVICE_URL`, `ETL_PYTHON_SERVICE_TOKEN`, `SUPABASE_*`, `ALLOWED_ORIGINS`

## 🔒 Security Best Practices

1. **Encryption**: All database credentials are encrypted at rest
2. **Query Sanitization**: All queries are sanitized to prevent SQL injection
3. **Audit Logging**: All operations are logged for audit purposes
4. **JWT Validation**: All endpoints validate JWT tokens
5. **CORS**: Configured to allow only trusted origins
6. **Environment Variables**: Never commit secrets to version control

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify PostgreSQL is running
   - Check DATABASE_URL in .env
   - Ensure database exists

2. **Redis Connection Errors**
   - Verify Redis is running
   - Check REDIS_HOST and REDIS_PORT
   - Test connection: `redis-cli ping`

3. **Migration Errors**
   - Check database permissions
   - Verify schema changes
   - Review migration files

4. **Authentication Errors**
   - Verify Supabase credentials
   - Check JWT token validity
   - Ensure guards are properly configured

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style

- Follow NestJS conventions
- Use Biome for formatting
- Write tests for new features
- Update documentation
- Follow TypeScript best practices

## 📝 License

This project is part of the MantrixFlow platform.

## 🔗 Useful Links

- [NestJS Documentation](https://docs.nestjs.com)
- [Drizzle ORM Documentation](https://orm.drizzle.team)
- [BullMQ Documentation](https://docs.bullmq.io)
- [Supabase Documentation](https://supabase.com/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs)

---

**Built with ❤️ for MantrixFlow - Transforming Data into Insights**
