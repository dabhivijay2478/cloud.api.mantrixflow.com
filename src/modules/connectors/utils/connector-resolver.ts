import * as rawConnectorsConfig from '../../../config/connectors.json';

export interface SourceConnectorDefinition {
  id: string;
  type?: string;
  label: string;
  aliases?: string[];
  registry_type?: string;
  cdc?: boolean;
  cdc_providers?: Array<{ id: string; label: string; instructions?: Record<string, unknown> }>;
  cdc_verify_steps?: string[];
}

export interface DestinationConnectorDefinition {
  id: string;
  type?: string;
  label: string;
  aliases?: string[];
  registry_type?: string;
}

interface ConnectorsConfig {
  sources?: SourceConnectorDefinition[];
  destinations?: DestinationConnectorDefinition[];
}

export interface ResolvedSourceConnectorType {
  connector?: SourceConnectorDefinition;
  canonicalType: string;
  registryType: string;
  etlType: string;
  supportsCdc: boolean;
}

export interface ResolvedDestinationConnectorType {
  connector?: DestinationConnectorDefinition;
  canonicalType: string;
  registryType: string;
  etlType: string;
}

const connectorsConfig = rawConnectorsConfig as ConnectorsConfig;
const sourceConnectors = connectorsConfig.sources ?? [];
const destinationConnectors = connectorsConfig.destinations ?? [];

function normalizeValue(value?: string | null): string {
  return (value ?? '').trim().toLowerCase();
}

function stripConnectorPrefix(value: string): string {
  let normalized = value;
  for (const prefix of ['source-', 'target-', 'destination-']) {
    if (normalized.startsWith(prefix)) {
      normalized = normalized.slice(prefix.length);
    }
  }
  return normalized;
}

function buildLookupVariants(value?: string | null): string[] {
  const normalized = normalizeValue(value);
  if (!normalized) {
    return [];
  }

  const stripped = stripConnectorPrefix(normalized);
  const variants = new Set<string>([normalized]);

  if (stripped) {
    variants.add(stripped);
    variants.add(`source-${stripped}`);
    variants.add(`destination-${stripped}`);
    variants.add(`target-${stripped}`);
  }

  return Array.from(variants);
}

function connectorTerms(connector: {
  id: string;
  type?: string;
  aliases?: string[];
  registry_type?: string;
}): Set<string> {
  return new Set(
    [connector.id, connector.type, connector.registry_type, ...(connector.aliases ?? [])]
      .flatMap((term) => buildLookupVariants(term))
      .filter(Boolean),
  );
}

function matchesConnector(
  connector: {
    id: string;
    type?: string;
    aliases?: string[];
    registry_type?: string;
  },
  lookupVariants: Set<string>,
): boolean {
  const terms = connectorTerms(connector);
  return Array.from(lookupVariants).some((variant) => terms.has(variant));
}

function inferCanonicalType(connector: { id: string; type?: string }): string {
  return normalizeValue(connector.type) || stripConnectorPrefix(normalizeValue(connector.id));
}

export function normalizeConnectorType(type?: string | null): string {
  return stripConnectorPrefix(normalizeValue(type));
}

export function findSourceConnector(type?: string | null): SourceConnectorDefinition | undefined {
  const lookupVariants = new Set(buildLookupVariants(type));
  if (lookupVariants.size === 0) {
    return undefined;
  }

  return sourceConnectors.find((connector) => matchesConnector(connector, lookupVariants));
}

export function findDestinationConnector(
  type?: string | null,
): DestinationConnectorDefinition | undefined {
  const lookupVariants = new Set(buildLookupVariants(type));
  if (lookupVariants.size === 0) {
    return undefined;
  }

  return destinationConnectors.find((connector) => matchesConnector(connector, lookupVariants));
}

export function resolveSourceConnectorType(type?: string | null): ResolvedSourceConnectorType {
  const normalized = normalizeConnectorType(type) || 'postgres';
  const connector = findSourceConnector(type);
  const canonicalType = connector ? inferCanonicalType(connector) : normalized;
  const registryType = normalizeValue(connector?.registry_type) || canonicalType;
  const connectorId = normalizeValue(connector?.id);

  return {
    connector,
    canonicalType,
    registryType,
    etlType: connectorId.startsWith('source-') ? connectorId : `source-${canonicalType}`,
    supportsCdc: Boolean(connector?.cdc),
  };
}

export function resolveDestinationConnectorType(
  type?: string | null,
): ResolvedDestinationConnectorType {
  const normalized = normalizeConnectorType(type) || 'postgres';
  const connector = findDestinationConnector(type);
  const canonicalType = connector ? inferCanonicalType(connector) : normalized;
  const registryType = normalizeValue(connector?.registry_type) || canonicalType;

  return {
    connector,
    canonicalType,
    registryType,
    etlType: registryType,
  };
}

export function listSupportedSourceConnectorTypes(): string[] {
  return Array.from(
    new Set(sourceConnectors.map((connector) => inferCanonicalType(connector))),
  ).sort();
}

export function listSupportedDestinationConnectorTypes(): string[] {
  return Array.from(
    new Set(destinationConnectors.map((connector) => inferCanonicalType(connector))),
  ).sort();
}
