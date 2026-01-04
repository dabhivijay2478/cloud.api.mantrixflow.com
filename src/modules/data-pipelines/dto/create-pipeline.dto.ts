import {
    IsString,
    IsNotEmpty,
    IsOptional,
    IsEnum,
    IsArray,
    ValidateNested,
    IsBoolean,
    IsObject,
    IsUUID,
    MaxLength,
} from 'class-validator';
import { Type } from 'class-transformer';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Column Mapping DTO
 */
export class ColumnMappingDto {
    @ApiProperty({ description: 'Source column name' })
    @IsString()
    @IsNotEmpty()
    sourceColumn: string;

    @ApiProperty({ description: 'Destination column name' })
    @IsString()
    @IsNotEmpty()
    destinationColumn: string;

    @ApiProperty({ description: 'PostgreSQL data type' })
    @IsString()
    @IsNotEmpty()
    dataType: string;

    @ApiProperty({ description: 'Whether column is nullable' })
    @IsBoolean()
    nullable: boolean;

    @ApiPropertyOptional({ description: 'Default value for column' })
    @IsString()
    @IsOptional()
    defaultValue?: string;

    @ApiPropertyOptional({ description: 'Whether column is primary key' })
    @IsBoolean()
    @IsOptional()
    isPrimaryKey?: boolean;

    @ApiPropertyOptional({ description: 'Maximum length for VARCHAR columns' })
    @IsOptional()
    maxLength?: number;
}

/**
 * Transformation DTO
 */
export class TransformationDto {
    @ApiProperty({ description: 'Source column name' })
    @IsString()
    @IsNotEmpty()
    sourceColumn: string;

    @ApiProperty({
        description: 'Type of transformation',
        enum: ['rename', 'cast', 'concat', 'split', 'custom'],
    })
    @IsEnum(['rename', 'cast', 'concat', 'split', 'custom'])
    transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';

    @ApiProperty({ description: 'Transformation configuration' })
    @IsObject()
    transformConfig: any;

    @ApiProperty({ description: 'Destination column name' })
    @IsString()
    @IsNotEmpty()
    destinationColumn: string;
}

/**
 * Create Pipeline DTO
 */
export class CreatePipelineDto {
    @ApiProperty({ description: 'Pipeline name' })
    @IsString()
    @IsNotEmpty()
    @MaxLength(255)
    name: string;

    @ApiPropertyOptional({ description: 'Pipeline description' })
    @IsString()
    @IsOptional()
    description?: string;

    // Source configuration
    @ApiProperty({
        description: 'Source type',
        example: 'postgres',
        enum: ['postgres', 'stripe', 'salesforce', 'google_sheets'],
    })
    @IsString()
    @IsNotEmpty()
    sourceType: string;

    @ApiPropertyOptional({
        description: 'Source connection ID (for postgres source)',
    })
    @IsUUID()
    @IsOptional()
    sourceConnectionId?: string;

    @ApiPropertyOptional({
        description: 'Source configuration (for external sources)',
    })
    @IsObject()
    @IsOptional()
    sourceConfig?: any;

    @ApiPropertyOptional({ description: 'Source schema name' })
    @IsString()
    @IsOptional()
    sourceSchema?: string;

    @ApiPropertyOptional({ description: 'Source table name' })
    @IsString()
    @IsOptional()
    sourceTable?: string;

    @ApiPropertyOptional({ description: 'Custom SQL query for source' })
    @IsString()
    @IsOptional()
    sourceQuery?: string;

    // Destination configuration
    @ApiProperty({ description: 'Destination connection ID (PostgreSQL)' })
    @IsUUID()
    @IsNotEmpty()
    destinationConnectionId: string;

    @ApiPropertyOptional({
        description: 'Destination schema name',
        default: 'public',
    })
    @IsString()
    @IsOptional()
    destinationSchema?: string;

    @ApiProperty({ description: 'Destination table name' })
    @IsString()
    @IsNotEmpty()
    @MaxLength(255)
    destinationTable: string;

    // Schema mapping
    @ApiPropertyOptional({
        description: 'Column mappings from source to destination',
        type: [ColumnMappingDto],
    })
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => ColumnMappingDto)
    @IsOptional()
    columnMappings?: ColumnMappingDto[];

    @ApiPropertyOptional({
        description: 'Data transformations',
        type: [TransformationDto],
    })
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => TransformationDto)
    @IsOptional()
    transformations?: TransformationDto[];

    // Write mode
    @ApiPropertyOptional({
        description: 'Write mode',
        enum: ['append', 'upsert', 'replace'],
        default: 'append',
    })
    @IsEnum(['append', 'upsert', 'replace'])
    @IsOptional()
    writeMode?: 'append' | 'upsert' | 'replace';

    @ApiPropertyOptional({
        description: 'Upsert key columns (required for upsert mode)',
        type: [String],
    })
    @IsArray()
    @IsString({ each: true })
    @IsOptional()
    upsertKey?: string[];

    // Sync configuration
    @ApiPropertyOptional({
        description: 'Sync mode',
        enum: ['full', 'incremental'],
        default: 'full',
    })
    @IsEnum(['full', 'incremental'])
    @IsOptional()
    syncMode?: 'full' | 'incremental';

    @ApiPropertyOptional({
        description: 'Incremental column (required for incremental sync)',
    })
    @IsString()
    @IsOptional()
    incrementalColumn?: string;

    @ApiPropertyOptional({
        description: 'Sync frequency',
        enum: ['manual', '15min', '1hour', '24hours'],
        default: 'manual',
    })
    @IsEnum(['manual', '15min', '1hour', '24hours'])
    @IsOptional()
    syncFrequency?: 'manual' | '15min' | '1hour' | '24hours';

    // Collector configuration
    @ApiPropertyOptional({
        description: 'Collector configurations',
        type: 'array',
        isArray: true,
    })
    @IsArray()
    @IsOptional()
    collectors?: Array<{
        id: string;
        sourceId: string;
        selectedTables: string[];
        transformers?: Array<{
            id: string;
            name: string;
            collectorId?: string;
            emitterId?: string;
            fieldMappings?: Array<{ source: string; destination: string }>; // JSON array format
        }>;
    }>;

    // Emitter configuration
    @ApiPropertyOptional({
        description: 'Emitter configurations. Emitters use existing connections via destinationId - connectionConfig is ignored.',
        type: 'array',
        isArray: true,
    })
    @IsArray()
    @IsOptional()
    emitters?: Array<{
        id: string;
        transformId: string;
        destinationId: string; // References existing connection (like collectors use sourceId)
        destinationName: string;
        destinationType: string;
        connectionConfig?: Record<string, string>; // Optional, ignored - connection is referenced by destinationId
    }>;
}

/**
 * Update Pipeline DTO
 */
export class UpdatePipelineDto {
    @ApiPropertyOptional({ description: 'Pipeline name' })
    @IsString()
    @IsOptional()
    @MaxLength(255)
    name?: string;

    @ApiPropertyOptional({ description: 'Pipeline description' })
    @IsString()
    @IsOptional()
    description?: string;

    @ApiPropertyOptional({
        description: 'Column mappings',
        type: [ColumnMappingDto],
    })
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => ColumnMappingDto)
    @IsOptional()
    columnMappings?: ColumnMappingDto[];

    @ApiPropertyOptional({
        description: 'Data transformations',
        type: [TransformationDto],
    })
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => TransformationDto)
    @IsOptional()
    transformations?: TransformationDto[];

    @ApiPropertyOptional({
        description: 'Write mode',
        enum: ['append', 'upsert', 'replace'],
    })
    @IsEnum(['append', 'upsert', 'replace'])
    @IsOptional()
    writeMode?: 'append' | 'upsert' | 'replace';

    @ApiPropertyOptional({
        description: 'Upsert key columns',
        type: [String],
    })
    @IsArray()
    @IsString({ each: true })
    @IsOptional()
    upsertKey?: string[];

    @ApiPropertyOptional({
        description: 'Sync mode',
        enum: ['full', 'incremental'],
    })
    @IsEnum(['full', 'incremental'])
    @IsOptional()
    syncMode?: 'full' | 'incremental';

    @ApiPropertyOptional({ description: 'Incremental column' })
    @IsString()
    @IsOptional()
    incrementalColumn?: string;

    @ApiPropertyOptional({
        description: 'Sync frequency',
        enum: ['manual', '15min', '1hour', '24hours'],
    })
    @IsEnum(['manual', '15min', '1hour', '24hours'])
    @IsOptional()
    syncFrequency?: 'manual' | '15min' | '1hour' | '24hours';

    @ApiPropertyOptional({
        description: 'Pipeline status',
        enum: ['active', 'paused'],
    })
    @IsEnum(['active', 'paused'])
    @IsOptional()
    status?: 'active' | 'paused';

    // Collector configuration (for updating pipeline transformations)
    @ApiPropertyOptional({
        description: 'Collector configurations',
        type: 'array',
        isArray: true,
    })
    @IsArray()
    @IsOptional()
    collectors?: Array<{
        id: string;
        sourceId: string;
        selectedTables: string[];
        transformers?: Array<{
            id: string;
            name: string;
            collectorId?: string;
            emitterId?: string;
            fieldMappings?: Array<{ source: string; destination: string }>; // JSON array format
        }>;
    }>;

    // Emitter configuration (for updating pipeline transformations)
    @ApiPropertyOptional({
        description: 'Emitter configurations. Emitters use existing connections via destinationId - connectionConfig is ignored.',
        type: 'array',
        isArray: true,
    })
    @IsArray()
    @IsOptional()
    emitters?: Array<{
        id: string;
        transformId: string;
        destinationId: string; // References existing connection (like collectors use sourceId)
        destinationName: string;
        destinationType: string;
        connectionConfig?: Record<string, string>; // Optional, ignored - connection is referenced by destinationId
    }>;
}
