import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsArray, IsNumber, IsOptional, Min, Max } from 'class-validator';

export class ExecuteQueryDto {
  @ApiProperty({
    description: 'SQL query (SELECT only)',
    example: 'SELECT * FROM users LIMIT 10',
  })
  @IsString()
  query: string;

  @ApiProperty({
    description: 'Query parameters (for parameterized queries)',
    type: [String],
    required: false,
    example: ['value1', 'value2'],
  })
  @IsArray()
  @IsOptional()
  params?: any[];

  @ApiProperty({
    description: 'Query timeout in milliseconds',
    example: 60000,
    required: false,
  })
  @IsNumber()
  @Min(1000)
  @IsOptional()
  timeout?: number;
}

export class QueryExecutionResponseDto {
  @ApiProperty({ description: 'Query results', type: [Object] })
  rows: any[];

  @ApiProperty({ description: 'Number of rows returned', example: 10 })
  rowCount: number;

  @ApiProperty({
    description: 'Column information',
    type: 'array',
    items: {
      type: 'object',
      properties: {
        name: { type: 'string' },
        dataType: { type: 'string' },
      },
    },
  })
  columns: Array<{ name: string; dataType: string }>;

  @ApiProperty({ description: 'Execution time in milliseconds', example: 45 })
  executionTimeMs: number;

  @ApiProperty({ description: 'Query execution plan (if explain was requested)', required: false })
  queryPlan?: any;
}

