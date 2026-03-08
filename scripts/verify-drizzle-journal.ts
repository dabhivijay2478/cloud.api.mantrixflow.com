import * as fs from 'node:fs';
import * as path from 'node:path';

type JournalEntry = {
  tag: string;
};

type Journal = {
  entries: JournalEntry[];
};

const migrationsDir = path.join(import.meta.dir, '..', 'src', 'database', 'drizzle', 'migrations');
const journalPath = path.join(migrationsDir, 'meta', '_journal.json');

const migrationFiles = fs
  .readdirSync(migrationsDir)
  .filter((name) => name.endsWith('.sql'))
  .map((name) => name.replace(/\.sql$/, ''))
  .sort();

const journal = JSON.parse(fs.readFileSync(journalPath, 'utf8')) as Journal;
const journalTags = new Set(journal.entries.map((entry) => entry.tag));

const parseMigrationNumber = (tag: string) => {
  const match = /^(\d+)_/.exec(tag);
  return match ? Number.parseInt(match[1], 10) : null;
};

const journalNumbers = journal.entries
  .map((entry) => parseMigrationNumber(entry.tag))
  .filter((value): value is number => value !== null);
const maxJournalNumber = journalNumbers.length > 0 ? Math.max(...journalNumbers) : -1;

const newerMissingFromJournal = migrationFiles.filter((file) => {
  if (journalTags.has(file)) {
    return false;
  }

  const migrationNumber = parseMigrationNumber(file);
  return migrationNumber !== null && migrationNumber > maxJournalNumber;
});

if (newerMissingFromJournal.length > 0) {
  console.error('Missing Drizzle journal entries for newer migration files:');
  for (const file of newerMissingFromJournal) {
    console.error(`- ${file}`);
  }
  process.exit(1);
}

console.log(`Drizzle journal is up to date through migration ${maxJournalNumber}.`);
