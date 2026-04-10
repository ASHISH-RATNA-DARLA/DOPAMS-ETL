const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

function loadEnvFile(filePath) {
  const env = {};
  if (!fs.existsSync(filePath)) return env;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const idx = trimmed.indexOf('=');
    if (idx === -1) continue;
    const key = trimmed.slice(0, idx).trim();
    const val = trimmed.slice(idx + 1).trim();
    env[key] = val;
  }
  return env;
}

function pickEnv() {
  const root = process.cwd();
  const candidates = [
    path.join(root, '.env.server'),
    path.join(root, '.env'),
    path.join(root, 'etl-files', 'etl_files_media_server', '.env'),
  ];

  for (const c of candidates) {
    if (fs.existsSync(c)) {
      const loaded = loadEnvFile(c);
      if (loaded.POSTGRES_HOST && loaded.POSTGRES_DB && loaded.POSTGRES_USER) {
        return { env: loaded, source: c };
      }
    }
  }

  return { env: process.env, source: 'process.env' };
}

async function run() {
  const { env, source } = pickEnv();
  const client = new Client({
    host: env.POSTGRES_HOST,
    port: Number(env.POSTGRES_PORT || 5432),
    database: env.POSTGRES_DB,
    user: env.POSTGRES_USER,
    password: env.POSTGRES_PASSWORD,
    ssl: false,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    envSource: source,
    db: {
      host: env.POSTGRES_HOST,
      port: Number(env.POSTGRES_PORT || 5432),
      database: env.POSTGRES_DB,
      user: env.POSTGRES_USER,
    },
    findings: {},
  };

  try {
    await client.connect();

    const queries = {
      totalRows: `SELECT COUNT(*)::bigint AS total FROM files`,
      recent: `
        SELECT id, source_type, source_field, parent_id, file_id, has_field, is_empty,
               is_downloaded, download_attempts, left(coalesce(download_error,''), 180) AS download_error,
               created_at, downloaded_at
        FROM files
        ORDER BY created_at DESC NULLS LAST
        LIMIT 25
      `,
      pendingByTypeField: `
        SELECT source_type, source_field, COUNT(*)::bigint AS pending_count
        FROM files
        WHERE file_id IS NOT NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
          AND (is_downloaded IS FALSE OR downloaded_at IS NULL)
        GROUP BY source_type, source_field
        ORDER BY pending_count DESC, source_type, source_field
      `,
      nullFileIdByTypeField: `
        SELECT source_type, source_field,
               COUNT(*)::bigint AS total,
               COUNT(*) FILTER (WHERE file_id IS NULL)::bigint AS null_file_id,
               COUNT(*) FILTER (WHERE file_id IS NOT NULL)::bigint AS non_null_file_id,
               COUNT(*) FILTER (WHERE has_field IS TRUE AND is_empty IS FALSE AND file_id IS NULL)::bigint AS suspicious_null_file_id
        FROM files
        GROUP BY source_type, source_field
        ORDER BY total DESC
      `,
      errorSummary: `
        SELECT source_type, source_field,
               COUNT(*)::bigint AS error_rows,
               COUNT(*) FILTER (WHERE download_attempts > 0)::bigint AS attempted_rows,
               MAX(download_attempts) AS max_attempts
        FROM files
        WHERE download_error IS NOT NULL
        GROUP BY source_type, source_field
        ORDER BY error_rows DESC, source_type, source_field
      `,
      topErrors: `
        SELECT left(download_error, 140) AS error_text, COUNT(*)::bigint AS cnt
        FROM files
        WHERE download_error IS NOT NULL
        GROUP BY left(download_error, 140)
        ORDER BY cnt DESC
        LIMIT 15
      `,
      distribution: `
        SELECT source_type, source_field, COUNT(*)::bigint AS cnt
        FROM files
        GROUP BY source_type, source_field
        ORDER BY source_type, source_field
      `,
      stuckRetries: `
        SELECT source_type, source_field, COUNT(*)::bigint AS stuck_count
        FROM files
        WHERE file_id IS NOT NULL
          AND is_downloaded IS FALSE
          AND download_attempts >= 3
        GROUP BY source_type, source_field
        ORDER BY stuck_count DESC
      `,
      statusMix: `
        SELECT
          COUNT(*)::bigint AS total,
          COUNT(*) FILTER (WHERE file_id IS NOT NULL)::bigint AS with_file_id,
          COUNT(*) FILTER (WHERE file_id IS NOT NULL AND has_field IS TRUE AND is_empty IS FALSE)::bigint AS candidate_downloadable,
          COUNT(*) FILTER (WHERE is_downloaded IS TRUE)::bigint AS downloaded_true,
          COUNT(*) FILTER (WHERE is_downloaded IS FALSE OR downloaded_at IS NULL)::bigint AS pending_or_unknown,
          COUNT(*) FILTER (WHERE download_error IS NOT NULL)::bigint AS with_error
        FROM files
      `,
      duplicateFileId: `
        SELECT file_id, COUNT(*)::bigint AS cnt
        FROM files
        WHERE file_id IS NOT NULL
        GROUP BY file_id
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 20
      `,
      unmappablePotential: `
        SELECT source_type, source_field, COUNT(*)::bigint AS cnt
        FROM files
        WHERE file_id IS NOT NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
          AND source_type NOT IN ('crime','person','property','interrogation','mo_seizures','chargesheets','case_property')
        GROUP BY source_type, source_field
        ORDER BY cnt DESC
      `,
    };

    for (const [k, q] of Object.entries(queries)) {
      const res = await client.query(q);
      report.findings[k] = res.rows;
    }

    const outPath = path.join(process.cwd(), 'diagnostics', 'files_rca_report.json');
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2), 'utf8');

    console.log(`Report written: ${outPath}`);
    console.log(`Total rows in files: ${report.findings.totalRows?.[0]?.total || '0'}`);
    console.log(`Pending groups: ${report.findings.pendingByTypeField?.length || 0}`);
    console.log(`Error groups: ${report.findings.errorSummary?.length || 0}`);
    console.log('Top pending groups:');
    for (const row of (report.findings.pendingByTypeField || []).slice(0, 8)) {
      console.log(`  - ${row.source_type}/${row.source_field}: ${row.pending_count}`);
    }
  } finally {
    await client.end();
  }
}

run().catch((err) => {
  console.error('RCA diagnostics failed:', err.message);
  process.exit(1);
});
