import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  IsUUID,
  MaxLength,
  ValidateNested,
} from 'class-validator';

/**
 * Field Mapping DTO (for transformers)
 */
export class FieldMappingDto {
  @ApiProperty({ description: 'Source field path' })
  @IsString()
  @IsNotEmpty()
  source: string;

  @ApiProperty({ description: 'Destination field name' })
  @IsString()
  @IsNotEmpty()
  destination: string;
}

/**
 * Transformer DTO (within collectors)
 */
export class TransformerDto {
  @ApiProperty({ description: 'Transformer ID' })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({ description: 'Transformer name' })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiPropertyOptional({ description: 'Collector ID this transformer belongs to' })
  @IsString()
  @IsOptional()
  collectorId?: string;

  @ApiPropertyOptional({ description: 'Emitter ID this transformer belongs to' })
  @IsString()
  @IsOptional()
  emitterId?: string;

  @ApiPropertyOptional({
    description: 'Field mappings (object format: { "source": "destination" } or array format)',
  })
  @IsOptional()
  // Note: fieldMappings can be either object or array, validation happens in controller
  // Using any to allow both formats - controller will transform to array format
  // We use @IsOptional() to whitelist the property, actual validation happens in controller
  fieldMappings?: any;
}

/**
 * Collector DTO
 */
export class CollectorDto {
  @ApiProperty({ description: 'Collector ID' })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({ description: 'Source connection ID' })
  @IsUUID()
  @IsNotEmpty()
  sourceId: string;

  @ApiProperty({ description: 'Selected tables', type: [String] })
  @IsArray()
  @IsString({ each: true })
  @IsNotEmpty()
  selectedTables: string[];

  @ApiPropertyOptional({
    description: 'Transformers for this collector',
    type: [TransformerDto],
  })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TransformerDto)
  @IsOptional()
  transformers?: TransformerDto[];
}

/**
 * Emitter DTO
 */
export class EmitterDto {
  @ApiProperty({ description: 'Emitter ID' })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({ description: 'Transformer ID this emitter uses' })
  @IsString()
  @IsNotEmpty()
  transformId: string;

  @ApiProperty({ description: 'Destination connection ID' })
  @IsUUID()
  @IsNotEmpty()
  destinationId: string;

  @ApiProperty({ description: 'Destination name' })
  @IsString()
  @IsNotEmpty()
  destinationName: string;

  @ApiProperty({ description: 'Destination type' })
  @IsString()
  @IsNotEmpty()
  destinationType: string;

  @ApiPropertyOptional({
    description: 'Connection config (ignored - connection is referenced by destinationId)',
  })
  @IsObject()
  @IsOptional()
  connectionConfig?: Record<string, string>;
}

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
    description: 'Source data source ID (for postgres source)',
  })
  @IsUUID()
  @IsOptional()
  sourceDataSourceId?: string;

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
  @ApiProperty({ description: 'Destination data source ID (PostgreSQL)' })
  @IsUUID()
  @IsNotEmpty()
  destinationDataSourceId: string;

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
  @ValidateNested({ each: true })
  @Type(() => CollectorDto)
  @IsOptional()
  collectors?: CollectorDto[];

  // Emitter configuration
  @ApiPropertyOptional({
    description:
      'Emitter configurations. Emitters use existing connections via destinationId - connectionConfig is ignored.',
    type: 'array',
    isArray: true,
  })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => EmitterDto)
  @IsOptional()
  emitters?: EmitterDto[];
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
  @ValidateNested({ each: true })
  @Type(() => CollectorDto)
  @IsOptional()
  collectors?: CollectorDto[];

  // Emitter configuration (for updating pipeline transformations)
  @ApiPropertyOptional({
    description:
      'Emitter configurations. Emitters use existing connections via destinationId - connectionConfig is ignored.',
    type: 'array',
    isArray: true,
  })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => EmitterDto)
  @IsOptional()
  emitters?: EmitterDto[];
}
