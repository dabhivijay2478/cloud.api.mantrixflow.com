import { ApiProperty } from '@nestjs/swagger';
import { IsBoolean, IsNumber, IsOptional, IsString, Max, Min, ValidateIf } from 'class-validator';

export class SSLConfigDto {
  @ApiProperty({
    description: 'Enable SSL connection',
    example: false,
    default: false,
  })
  @IsBoolean()
  enabled: boolean;

  @ApiProperty({ description: 'CA certificate (PEM format)', required: false })
  @IsString()
  @IsOptional()
  caCert?: string;

  @ApiProperty({
    description: 'Reject unauthorized certificates',
    example: true,
    default: true,
  })
  @IsBoolean()
  @IsOptional()
  rejectUnauthorized?: boolean;
}

export class SSHTunnelConfigDto {
  @ApiProperty({
    description: 'Enable SSH tunnel',
    example: false,
    default: false,
  })
  @IsBoolean()
  enabled: boolean;

  @ApiProperty({ description: 'SSH host', example: 'ssh.example.com' })
  @IsString()
  host: string;

  @ApiProperty({ description: 'SSH port', example: 22, default: 22 })
  @IsNumber()
  @Min(1)
  @Max(65535)
  port: number;

  @ApiProperty({ description: 'SSH username', example: 'user' })
  @IsString()
  username: string;

  @ApiProperty({ description: 'SSH private key (PEM format)' })
  @IsString()
  privateKey: string;
}

export class TestConnectionDto {
  @ApiProperty({
    description:
      'PostgreSQL connection string (postgresql://user:password@host:port/database). If provided, individual connection fields are optional.',
    example: 'postgresql://postgres:password123@localhost:5432/mydb',
    required: false,
  })
  @IsString()
  @IsOptional()
  connectionString?: string;

  @ApiProperty({
    description: 'PostgreSQL host (required if connectionString is not provided)',
    example: 'localhost',
    required: false,
  })
  @ValidateIf((o) => !o.connectionString)
  @IsString()
  host?: string;

  @ApiProperty({ description: 'PostgreSQL port', example: 5432, default: 5432 })
  @IsNumber()
  @Min(1)
  @Max(65535)
  @IsOptional()
  port?: number;

  @ApiProperty({
    description: 'Database name (required if connectionString is not provided)',
    example: 'mydb',
    required: false,
  })
  @ValidateIf((o) => !o.connectionString)
  @IsString()
  database?: string;

  @ApiProperty({
    description: 'Database username (required if connectionString is not provided)',
    example: 'postgres',
    required: false,
  })
  @ValidateIf((o) => !o.connectionString)
  @IsString()
  username?: string;

  @ApiProperty({
    description: 'Database password (required if connectionString is not provided)',
    example: 'password123',
    required: false,
  })
  @ValidateIf((o) => !o.connectionString)
  @IsString()
  password?: string;

  @ApiProperty({
    description: 'SSL configuration',
    type: SSLConfigDto,
    required: false,
    nullable: true,
  })
  @IsOptional()
  ssl?: SSLConfigDto | null;

  @ApiProperty({
    description: 'SSH tunnel configuration',
    type: SSHTunnelConfigDto,
    required: false,
    nullable: true,
  })
  @IsOptional()
  sshTunnel?: SSHTunnelConfigDto | null;

  @ApiProperty({
    description: 'Connection timeout in milliseconds',
    example: 30000,
    required: false,
  })
  @IsNumber()
  @IsOptional()
  connectionTimeout?: number;

  @ApiProperty({
    description: 'Query timeout in milliseconds',
    example: 60000,
    required: false,
  })
  @IsNumber()
  @IsOptional()
  queryTimeout?: number;

  @ApiProperty({
    description: 'Connection pool size',
    example: 5,
    minimum: 1,
    maximum: 10,
    required: false,
  })
  @IsNumber()
  @Min(1)
  @Max(10)
  @IsOptional()
  poolSize?: number;

  @ApiProperty({
    description: 'Database type: "neon", "supabase", or "other"',
    example: 'other',
    enum: ['neon', 'supabase', 'other'],
    required: false,
  })
  @IsString()
  @IsOptional()
  databaseType?: string;
}

export class TestConnectionResponseDto {
  @ApiProperty({ description: 'Connection test success', example: true })
  success: boolean;

  @ApiProperty({
    description: 'Error message if connection failed',
    required: false,
  })
  error?: string;

  @ApiProperty({
    description: 'PostgreSQL version',
    example: 'PostgreSQL 14.5',
    required: false,
  })
  version?: string;

  @ApiProperty({
    description: 'Response time in milliseconds',
    example: 45,
    required: false,
  })
  responseTimeMs?: number;
}
