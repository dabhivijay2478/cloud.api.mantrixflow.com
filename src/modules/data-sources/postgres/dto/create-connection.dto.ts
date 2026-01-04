import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsNotEmpty, IsObject, IsString, ValidateNested } from 'class-validator';
import { TestConnectionDto } from './test-connection.dto';

export class CreateConnectionDto {
  @ApiProperty({
    description: 'Connection name',
    example: 'Production Database',
  })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiProperty({
    description: 'Connection configuration',
    type: TestConnectionDto,
  })
  @IsObject()
  @ValidateNested()
  @Type(() => TestConnectionDto)
  config: TestConnectionDto;
}

export class ConnectionResponseDto {
  @ApiProperty({
    description: 'Connection ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  id: string;

  @ApiProperty({
    description: 'Organization ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  orgId: string;

  @ApiProperty({
    description: 'User ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  userId: string;

  @ApiProperty({
    description: 'Connection name',
    example: 'Production Database',
  })
  name: string;

  @ApiProperty({
    description: 'Connection status',
    enum: ['active', 'inactive', 'error'],
  })
  status: string;

  @ApiProperty({ description: 'Port', example: 5432 })
  port: number;

  @ApiProperty({ description: 'SSL enabled', example: false })
  sslEnabled: boolean;

  @ApiProperty({ description: 'SSH tunnel enabled', example: false })
  sshTunnelEnabled: boolean;

  @ApiProperty({ description: 'Connection pool size', example: 5 })
  connectionPoolSize: number;

  @ApiProperty({ description: 'Query timeout in seconds', example: 60 })
  queryTimeoutSeconds: number;

  @ApiProperty({ description: 'Last connected at', required: false })
  lastConnectedAt?: Date;

  @ApiProperty({ description: 'Created at' })
  createdAt: Date;

  @ApiProperty({ description: 'Updated at' })
  updatedAt: Date;
}
