/**
 * CDC Verification Service
 * Wraps ETL POST /cdc/verify and /cdc/verify-all.
 * Loads cdc_providers and cdc_verify_steps from connector config.
 * No provider auto-detection — provider is user-selected and stored in cdc_prerequisites_status.
 */

import { HttpService } from '@nestjs/axios';
import { ForbiddenException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../common/utils/etl-url';
import { ConnectorMetadataService } from '../connectors/connector-metadata.service';
import { resolveSourceConnectorType } from '../connectors/utils/connector-resolver';
import { OrganizationRoleService } from '../organizations/services/organization-role.service';
import { ConnectionService } from './connection.service';
import { DataSourceRepository } from './repositories/data-source.repository';
import { DataSourceConnectionRepository } from './repositories/data-source-connection.repository';

export interface CdcPrerequisitesStatus {
  overall: 'verified' | 'partial' | 'not_started' | 'failed';
  checked_at: string;
  wal_level_ok?: boolean;
  wal2json_ok?: boolean;
  replication_role_ok?: boolean;
  replication_test_ok?: boolean;
  provider_selected?: string;
  last_error?: string | null;
}

@Injectable()
export class CdcVerifyService {
  private readonly logger = new Logger(CdcVerifyService.name);
  private readonly baseUrl: string;
  private readonly token: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly connectionService: ConnectionService,
    private readonly connectorMetadataService: ConnectorMetadataService,
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly roleService: OrganizationRoleService,
  ) {
    const raw =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
      this.configService.get<string>('PYTHON_SERVICE_URL') ??
      '';
    this.baseUrl = normalizeEtlBaseUrl(raw);
    this.token =
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ??
      this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN') ??
      '';
  }

  private headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.token}`,
    };
  }

  /**
   * Get CDC status and available providers for a data source.
   */
  async getCdcStatus(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<{
    cdc_prerequisites_status: CdcPrerequisitesStatus | null;
    cdc_providers: Array<{ id: string; label: string; instructions?: Record<string, unknown> }>;
    cdc_verify_steps: string[];
  }> {
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not configured for this data source');
    }

    const setup = await this.connectorMetadataService.getCdcSetup(
      connection.connectionType || dataSource.sourceType,
    );

    const cdcPrerequisitesStatus =
      (connection.cdcPrerequisitesStatus as CdcPrerequisitesStatus) ?? null;

    return {
      cdc_prerequisites_status: cdcPrerequisitesStatus,
      cdc_providers: setup.cdc_providers ?? [],
      cdc_verify_steps: setup.cdc_verify_steps ?? [
        'wal_level',
        'wal2json',
        'replication_role',
        'replication_test',
      ],
    };
  }

  /**
   * Verify a single CDC step and update cdc_prerequisites_status.
   */
  async verifyStep(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    step: string,
    providerSelected?: string,
  ): Promise<{ ok: boolean; detail?: object; error?: string }> {
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can verify CDC prerequisites');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not configured for this data source');
    }

    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource || dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    const decryptedConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      dataSourceId,
      userId,
    );
    const sourceType = resolveSourceConnectorType(connection.connectionType).registryType;

    let result: { ok: boolean; [k: string]: unknown };
    try {
      const res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/cdc/verify`,
          {
            source_type: sourceType,
            connection_config: decryptedConfig,
            step,
          },
          { headers: this.headers(), timeout: 30000 },
        ),
      );
      result = res.data ?? {};
    } catch (error: unknown) {
      const err = error as {
        response?: { data?: { detail?: string; error?: string } };
        message?: string;
      };
      const msg =
        err?.response?.data?.detail ??
        err?.response?.data?.error ??
        err?.message ??
        'CDC verification failed';
      this.logger.warn(`CDC verify step=${step} failed: ${msg}`);
      const status = this.buildUpdatedStatus(
        connection.cdcPrerequisitesStatus,
        step,
        false,
        providerSelected,
        msg,
      );
      await this.connectionRepository.updateByDataSourceId(dataSourceId, {
        cdcPrerequisitesStatus: status as object,
      });
      return { ok: false, error: msg };
    }

    const ok = Boolean(result.ok);
    const status = this.buildUpdatedStatus(
      connection.cdcPrerequisitesStatus,
      step,
      ok,
      providerSelected,
      ok ? undefined : ((result.error as string) ?? (result.detail as string)),
    );
    await this.connectionRepository.updateByDataSourceId(dataSourceId, {
      cdcPrerequisitesStatus: status as object,
    });

    return {
      ok,
      detail: result,
      error: ok ? undefined : ((result.error as string) ?? (result.detail as string)),
    };
  }

  /**
   * Run all CDC verification steps and update cdc_prerequisites_status.
   */
  async verifyAll(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    providerSelected?: string,
  ): Promise<{ ok: boolean; steps?: Record<string, unknown>; overall?: string }> {
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can verify CDC prerequisites');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not configured for this data source');
    }

    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource || dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    const decryptedConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      dataSourceId,
      userId,
    );
    const sourceType = resolveSourceConnectorType(connection.connectionType).registryType;

    let result: { ok?: boolean; steps?: Record<string, unknown>; overall?: string };
    try {
      const res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/cdc/verify-all`,
          {
            source_type: sourceType,
            connection_config: decryptedConfig,
          },
          { headers: this.headers(), timeout: 60000 },
        ),
      );
      result = res.data ?? {};
    } catch (error: unknown) {
      const err = error as { response?: { data?: { detail?: string } }; message?: string };
      const msg = err?.response?.data?.detail ?? err?.message ?? 'CDC verification failed';
      this.logger.warn(`CDC verify-all failed: ${msg}`);
      const status: CdcPrerequisitesStatus = {
        overall: 'failed',
        checked_at: new Date().toISOString(),
        provider_selected: providerSelected,
        last_error: msg,
      };
      await this.connectionRepository.updateByDataSourceId(dataSourceId, {
        cdcPrerequisitesStatus: status as object,
      });
      return { ok: false, overall: 'failed' };
    }

    const ok = Boolean(result.ok);
    const steps = result.steps ?? {};
    const status: CdcPrerequisitesStatus = {
      overall:
        (result.overall as CdcPrerequisitesStatus['overall']) ?? (ok ? 'verified' : 'failed'),
      checked_at: new Date().toISOString(),
      wal_level_ok: (steps.wal_level as { ok?: boolean })?.ok,
      wal2json_ok: (steps.wal2json as { ok?: boolean })?.ok,
      replication_role_ok: (steps.replication_role as { ok?: boolean })?.ok,
      replication_test_ok: (steps.replication_test as { ok?: boolean })?.ok,
      provider_selected: providerSelected,
      last_error: ok
        ? null
        : (
            Object.values(steps).find((s) => (s as { ok?: boolean })?.ok === false) as {
              error?: string;
            }
          )?.error,
    };
    await this.connectionRepository.updateByDataSourceId(dataSourceId, {
      cdcPrerequisitesStatus: status as object,
    });

    return {
      ok,
      steps: result.steps as Record<string, unknown>,
      overall: result.overall,
    };
  }

  private buildUpdatedStatus(
    current: unknown,
    step: string,
    ok: boolean,
    providerSelected?: string,
    lastError?: string,
  ): CdcPrerequisitesStatus {
    const prev = (current as CdcPrerequisitesStatus) ?? {};
    const base: CdcPrerequisitesStatus = {
      overall: prev.overall ?? 'not_started',
      checked_at: new Date().toISOString(),
      wal_level_ok: prev.wal_level_ok,
      wal2json_ok: prev.wal2json_ok,
      replication_role_ok: prev.replication_role_ok,
      replication_test_ok: prev.replication_test_ok,
      provider_selected: providerSelected ?? prev.provider_selected,
      last_error: lastError ?? (ok ? null : prev.last_error),
    };

    switch (step) {
      case 'wal_level':
        base.wal_level_ok = ok;
        break;
      case 'wal2json':
        base.wal2json_ok = ok;
        break;
      case 'replication_role':
        base.replication_role_ok = ok;
        break;
      case 'replication_test':
        base.replication_test_ok = ok;
        break;
    }

    const allOk =
      base.wal_level_ok && base.wal2json_ok && base.replication_role_ok && base.replication_test_ok;
    base.overall = allOk ? 'verified' : ok ? 'partial' : 'failed';

    return base;
  }
}
