const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

function loadEnv(candidates) {
  for (const filePath of candidates) {
    if (!fs.existsSync(filePath)) continue;
    const env = {};
    for (const raw of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
      const line = raw.trim();
      if (!line || line.startsWith('#') || !line.includes('=')) continue;
      const i = line.indexOf('=');
      env[line.slice(0, i).trim()] = line.slice(i + 1).trim();
    }
    return env;
  }
  return process.env;
}

function mapDestinationSubdir(sourceType, sourceField) {
  const st = (sourceType || '').toLowerCase();
  const sf = (sourceField || '').toUpperCase();

  if (st === 'crime' && sf === 'FIR_COPY') return 'crimes';
  if (st === 'crime' && sf === 'MEDIA') return 'crimes';
  if (st === 'person' && sf === 'IDENTITY_DETAILS') return path.join('person', 'identitydetails');
  if (st === 'person' && sf === 'MEDIA') return path.join('person', 'media');
  if (st === 'property' && sf === 'MEDIA') return 'property';
  if (st === 'interrogation' && sf === 'MEDIA') return path.join('interrogations', 'media');
  if (st === 'interrogation' && sf === 'INTERROGATION_REPORT') return path.join('interrogations', 'interrogationreport');
  if (st === 'interrogation' && sf === 'DOPAMS_DATA') return path.join('interrogations', 'dopamsdata');
  if (st === 'mo_seizures' && sf === 'MO_MEDIA') return 'mo_seizures';
  if (st === 'chargesheets' && sf === 'UPLOADCHARGESHEET') return 'chargesheets';
  if (st === 'case_property' && sf === 'MEDIA') return 'fsl_case_property';
  return null;
}

async function main() {
  const root = process.cwd();
  const env = loadEnv([
    path.join(root, '.env.server'),
    path.join(root, '.env'),
    path.join(root, 'etl-files', 'etl_files_media_server', '.env')
  ]);

  const client = new Client({
    host: env.POSTGRES_HOST,
    port: Number(env.POSTGRES_PORT || 5432),
    database: env.POSTGRES_DB,
    user: env.POSTGRES_USER,
    password: env.POSTGRES_PASSWORD,
    ssl: false
  });

  await client.connect();
  try {
    const q = `
      SELECT source_type, source_field,
             COUNT(*)::bigint AS total,
             COUNT(*) FILTER (WHERE file_id IS NOT NULL AND has_field IS TRUE AND is_empty IS FALSE)::bigint AS downloadable,
             COUNT(*) FILTER (WHERE file_id IS NOT NULL AND has_field IS TRUE AND is_empty IS FALSE AND (is_downloaded IS FALSE OR downloaded_at IS NULL))::bigint AS pending
      FROM files
      GROUP BY source_type, source_field
      ORDER BY source_type, source_field
    `;

    const { rows } = await client.query(q);

    const analyzed = rows.map(r => {
      const mapped = mapDestinationSubdir(r.source_type, r.source_field);
      return {
        source_type: r.source_type,
        source_field: r.source_field,
        total: Number(r.total),
        downloadable: Number(r.downloadable),
        pending: Number(r.pending),
        mapped_subdir: mapped,
        mapping_ok: Boolean(mapped)
      };
    });

    const gaps = analyzed.filter(r => r.downloadable > 0 && !r.mapping_ok);

    const report = {
      generatedAt: new Date().toISOString(),
      totals: {
        combos: analyzed.length,
        combos_with_downloadable: analyzed.filter(r => r.downloadable > 0).length,
        mapping_gaps_for_downloadable: gaps.length,
      },
      combos: analyzed,
      mapping_gaps: gaps,
    };

    const outPath = path.join(root, 'diagnostics', 'files_etl_coverage_report.json');
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

    console.log('Report written:', outPath);
    console.log('Combos:', report.totals.combos);
    console.log('Combos with downloadable:', report.totals.combos_with_downloadable);
    console.log('Mapping gaps (downloadable only):', report.totals.mapping_gaps_for_downloadable);

    for (const row of analyzed) {
      if (row.downloadable > 0) {
        console.log(`- ${row.source_type}/${row.source_field}: downloadable=${row.downloadable}, pending=${row.pending}, mapped=${row.mapping_ok ? row.mapped_subdir : 'NO'}`);
      }
    }
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error('Coverage check failed:', err.message);
  process.exit(1);
});
