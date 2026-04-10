const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

function loadEnv(candidates) {
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    const env = {};
    for (const raw of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
      const line = raw.trim();
      if (!line || line.startsWith('#') || !line.includes('=')) continue;
      const i = line.indexOf('=');
      env[line.slice(0, i).trim()] = line.slice(i + 1).trim();
    }
    return { env, source: p };
  }
  return { env: process.env, source: 'process.env' };
}

async function run() {
  const root = process.cwd();
  const loaded = loadEnv([
    path.join(root, '.env.server'),
    path.join(root, '.env'),
    path.join(root, 'etl-files', 'etl_files_media_server', '.env'),
  ]);

  const env = loaded.env;

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
    envSource: loaded.source,
    checks: {},
  };

  try {
    await client.connect();

    const queries = {
      columns_present: `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='files'
          AND column_name IN ('source_type','source_field','parent_id','file_id','has_field','is_empty','file_path','file_url','is_downloaded','download_error','download_attempts','created_at','downloaded_at')
        ORDER BY column_name
      `,
      enum_source_type: `
        SELECT unnest(enum_range(NULL::source_type_enum))::text AS value
      `,
      enum_source_field: `
        SELECT unnest(enum_range(NULL::source_field_enum))::text AS value
      `,
      downloadable_unmapped_combos: `
        SELECT source_type, source_field, COUNT(*)::bigint AS cnt
        FROM files
        WHERE file_id IS NOT NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
          AND (
            (source_type='crime' AND source_field IN ('FIR_COPY','MEDIA')) OR
            (source_type='person' AND source_field IN ('IDENTITY_DETAILS','MEDIA')) OR
            (source_type='property' AND source_field='MEDIA') OR
            (source_type='interrogation' AND source_field IN ('MEDIA','INTERROGATION_REPORT','DOPAMS_DATA')) OR
            (source_type='mo_seizures' AND source_field='MO_MEDIA') OR
            (source_type='chargesheets' AND source_field='uploadChargeSheet') OR
            (source_type='case_property' AND source_field='MEDIA')
          ) IS NOT TRUE
        GROUP BY source_type, source_field
        ORDER BY cnt DESC
      `,
      null_fileid_but_downloadable_flag: `
        SELECT source_type, source_field, COUNT(*)::bigint AS cnt
        FROM files
        WHERE file_id IS NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
        GROUP BY source_type, source_field
        ORDER BY cnt DESC
      `,
      pending_queue_size: `
        SELECT COUNT(*)::bigint AS pending
        FROM files
        WHERE file_id IS NOT NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
          AND (is_downloaded IS FALSE OR downloaded_at IS NULL)
      `,
      permanent_error_rows: `
        SELECT COUNT(*)::bigint AS permanent_error_rows
        FROM files
        WHERE download_error LIKE 'PERMANENT:%'
      `,
      high_attempt_rows: `
        SELECT COUNT(*)::bigint AS gt5_attempt_rows
        FROM files
        WHERE COALESCE(download_attempts,0) >= 5
          AND (is_downloaded IS FALSE OR downloaded_at IS NULL)
      `,
      path_url_missing_for_downloadable: `
        SELECT source_type, source_field,
               COUNT(*) FILTER (WHERE file_path IS NULL)::bigint AS missing_path,
               COUNT(*) FILTER (WHERE file_url IS NULL)::bigint AS missing_url,
               COUNT(*)::bigint AS total
        FROM files
        WHERE file_id IS NOT NULL
          AND has_field IS TRUE
          AND is_empty IS FALSE
        GROUP BY source_type, source_field
        HAVING COUNT(*) FILTER (WHERE file_path IS NULL OR file_url IS NULL) > 0
        ORDER BY total DESC
      `,
      duplicate_composite_key_rows: `
        SELECT source_type, source_field, parent_id, file_id, COALESCE(file_index,-1) AS file_index_norm, COUNT(*)::bigint AS dup_cnt
        FROM files
        WHERE file_id IS NOT NULL
        GROUP BY source_type, source_field, parent_id, file_id, COALESCE(file_index,-1)
        HAVING COUNT(*) > 1
        ORDER BY dup_cnt DESC
        LIMIT 30
      `,
      trigger_exists: `
        SELECT tgname
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname='public' AND c.relname='files' AND tgname='trigger_auto_generate_file_paths' AND NOT t.tgisinternal
      `,
      retry_error_top: `
        SELECT left(coalesce(download_error,''), 120) AS error_text, COUNT(*)::bigint AS cnt
        FROM files
        WHERE download_error IS NOT NULL
        GROUP BY left(coalesce(download_error,''), 120)
        ORDER BY cnt DESC
        LIMIT 10
      `,
    };

    for (const [name, sql] of Object.entries(queries)) {
      const res = await client.query(sql);
      report.checks[name] = res.rows;
    }

    const out = path.join(root, 'diagnostics', 'etl_schema_robustness_report.json');
    fs.writeFileSync(out, JSON.stringify(report, null, 2));

    const pending = report.checks.pending_queue_size?.[0]?.pending ?? 'n/a';
    const gaps = report.checks.downloadable_unmapped_combos?.length ?? 0;
    const badFlags = report.checks.null_fileid_but_downloadable_flag?.reduce((a, r) => a + Number(r.cnt), 0) || 0;
    const dupRows = report.checks.duplicate_composite_key_rows?.length ?? 0;

    console.log('Report written:', out);
    console.log('Pending queue:', pending);
    console.log('Unmapped downloadable combos:', gaps);
    console.log('NULL file_id with has_field=true & is_empty=false:', badFlags);
    console.log('Duplicate composite-key groups (sampled):', dupRows);
  } finally {
    await client.end();
  }
}

run().catch((e) => {
  console.error('Robustness audit failed:', e.message);
  process.exit(1);
});
