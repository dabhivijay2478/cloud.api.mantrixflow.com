import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNumber, IsOptional, IsBoolean, IsObject, Min, Max } from 'class-validator';

export class SSLConfigDto {
  @ApiProperty({ description: 'Enable SSL connection', example: false, default: false })
  @IsBoolean()
  enabled: boolean;

  @ApiProperty({ description: 'CA certificate (PEM format)', required: false })
  @IsString()
  @IsOptional()
  caCert?: string;

  @ApiProperty({ description: 'Reject unauthorized certificates', example: true, default: true })
  @IsBoolean()
  @IsOptional()
  rejectUnauthorized?: boolean;
}

export class SSHTunnelConfigDto {
  @ApiProperty({ description: 'Enable SSH tunnel', example: false, default: false })
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
  @ApiProperty({ description: 'PostgreSQL host', example: 'localhost' })
  @IsString()
  host: string;

  @ApiProperty({ description: 'PostgreSQL port', example: 5432, default: 5432 })
  @IsNumber()
  @Min(1)
  @Max(65535)
  @IsOptional()
  port?: number;

  @ApiProperty({ description: 'Database name', example: 'mydb' })
  @IsString()
  database: string;

  @ApiProperty({ description: 'Database username', example: 'postgres' })
  @IsString()
  username: string;

  @ApiProperty({ description: 'Database password', example: 'password123' })
  @IsString()
  password: string;

  @ApiProperty({ description: 'SSL configuration', type: SSLConfigDto, required: false })
  @IsObject()
  @IsOptional()
  ssl?: SSLConfigDto;

  @ApiProperty({ description: 'SSH tunnel configuration', type: SSHTunnelConfigDto, required: false })
  @IsObject()
  @IsOptional()
  sshTunnel?: SSHTunnelConfigDto;

  @ApiProperty({ description: 'Connection timeout in milliseconds', example: 30000, required: false })
  @IsNumber()
  @IsOptional()
  connectionTimeout?: number;

  @ApiProperty({ description: 'Query timeout in milliseconds', example: 60000, required: false })
  @IsNumber()
  @IsOptional()
  queryTimeout?: number;

  @ApiProperty({ description: 'Connection pool size', example: 5, minimum: 1, maximum: 10, required: false })
  @IsNumber()
  @Min(1)
  @Max(10)
  @IsOptional()
  poolSize?: number;
}

export class TestConnectionResponseDto {
  @ApiProperty({ description: 'Connection test success', example: true })
  success: boolean;

  @ApiProperty({ description: 'Error message if connection failed', required: false })
  error?: string;

  @ApiProperty({ description: 'PostgreSQL version', example: 'PostgreSQL 14.5', required: false })
  version?: string;

  @ApiProperty({ description: 'Response time in milliseconds', example: 45, required: false })
  responseTimeMs?: number;
}

