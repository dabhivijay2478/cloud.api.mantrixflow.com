import { ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsBoolean,
  IsNumber,
  IsOptional,
  IsString,
  Max,
  Min,
  ValidateNested,
} from 'class-validator';

export class SSLConfigDto {
  @ApiPropertyOptional({
    description: 'Enable SSL connection',
    example: true,
  })
  @IsOptional()
  @IsBoolean()
  enabled?: boolean;

  @ApiPropertyOptional({
    description: 'SSL CA certificate (PEM format)',
    example: '-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----',
  })
  @IsOptional()
  @IsString()
  caCert?: string;

  @ApiPropertyOptional({
    description: 'Reject unauthorized SSL certificates',
    example: false,
  })
  @IsOptional()
  @IsBoolean()
  rejectUnauthorized?: boolean;
}

export class SSHTunnelConfigDto {
  @ApiPropertyOptional({
    description: 'Enable SSH tunnel',
    example: false,
  })
  @IsOptional()
  @IsBoolean()
  enabled?: boolean;

  @ApiPropertyOptional({
    description: 'SSH tunnel host',
    example: 'ssh.example.com',
  })
  @IsOptional()
  @IsString()
  host?: string;

  @ApiPropertyOptional({
    description: 'SSH tunnel port',
    example: 22,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(65535)
  port?: number;

  @ApiPropertyOptional({
    description: 'SSH username',
    example: 'sshuser',
  })
  @IsOptional()
  @IsString()
  username?: string;

  @ApiPropertyOptional({
    description: 'SSH private key (PEM format)',
    example: '-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----',
  })
  @IsOptional()
  @IsString()
  privateKey?: string;
}

export class UpdateConnectionDto {
  @ApiPropertyOptional({
    description: 'Connection name',
    example: 'Production Database',
  })
  @IsOptional()
  @IsString()
  name?: string;

  @ApiPropertyOptional({
    description: 'Database host',
    example: 'localhost',
  })
  @IsOptional()
  @IsString()
  host?: string;

  @ApiPropertyOptional({
    description: 'Database port',
    example: 5432,
    minimum: 1,
    maximum: 65535,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(65535)
  port?: number;

  @ApiPropertyOptional({
    description: 'Database name',
    example: 'mydb',
  })
  @IsOptional()
  @IsString()
  database?: string;

  @ApiPropertyOptional({
    description: 'Database username',
    example: 'postgres',
  })
  @IsOptional()
  @IsString()
  username?: string;

  @ApiPropertyOptional({
    description: 'Database password',
    example: 'password',
  })
  @IsOptional()
  @IsString()
  password?: string;

  @ApiPropertyOptional({
    description: 'SSL configuration',
    type: SSLConfigDto,
    nullable: true,
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => SSLConfigDto)
  ssl?: SSLConfigDto | null;

  @ApiPropertyOptional({
    description: 'SSH tunnel configuration',
    type: SSHTunnelConfigDto,
    nullable: true,
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => SSHTunnelConfigDto)
  sshTunnel?: SSHTunnelConfigDto | null;

  @ApiPropertyOptional({
    description: 'Connection timeout in milliseconds',
    example: 30000,
    minimum: 1000,
  })
  @IsOptional()
  @IsNumber()
  @Min(1000)
  connectionTimeout?: number;

  @ApiPropertyOptional({
    description: 'Query timeout in milliseconds',
    example: 60000,
    minimum: 1000,
  })
  @IsOptional()
  @IsNumber()
  @Min(1000)
  queryTimeout?: number;

  @ApiPropertyOptional({
    description: 'Connection pool size',
    example: 5,
    minimum: 1,
    maximum: 50,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(50)
  poolSize?: number;
}
