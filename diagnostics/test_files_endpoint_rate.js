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
      const idx = line.indexOf('=');
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      env[k] = v;
    }
    return { env, source: filePath };
  }
  return { env: process.env, source: 'process.env' };
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getSampleFileId(dbEnv) {
  const client = new Client({
    host: dbEnv.POSTGRES_HOST,
    port: Number(dbEnv.POSTGRES_PORT || 5432),
    database: dbEnv.POSTGRES_DB,
    user: dbEnv.POSTGRES_USER,
    password: dbEnv.POSTGRES_PASSWORD,
    ssl: false,
  });
  await client.connect();
  try {
    const r = await client.query(
      `SELECT file_id
       FROM files
       WHERE file_id IS NOT NULL
         AND is_downloaded = TRUE
       ORDER BY downloaded_at DESC NULLS LAST
       LIMIT 1`
    );
    return r.rows[0]?.file_id || null;
  } finally {
    await client.end();
  }
}

async function main() {
  const root = process.cwd();
  const dbCfg = loadEnv([
    path.join(root, '.env.server'),
    path.join(root, '.env'),
    path.join(root, 'etl-files', 'etl_files_media_server', '.env'),
  ]);

  const apiCfg = loadEnv([
    path.join(root, 'etl-files', 'etl_files_media_server', '.env'),
    path.join(root, '.env.server'),
    path.join(root, '.env'),
  ]);

  const env = { ...dbCfg.env, ...apiCfg.env };

  const baseUrl = (env.DOPAMAS_API_URL || '').replace(/\/$/, '');
  const apiKey = env.DOPAMAS_API_KEY || '';

  if (!baseUrl) {
    throw new Error('DOPAMAS_API_URL is not set in available env files');
  }
  if (!apiKey) {
    throw new Error('DOPAMAS_API_KEY is not set in available env files');
  }

  const fileId = await getSampleFileId(env);
  if (!fileId) {
    throw new Error('Could not find a downloaded file_id to test');
  }

  const url = `${baseUrl}/files/${fileId}`;
  const headers = { 'x-api-key': apiKey };

  console.log('Testing endpoint:', url);
  console.log('DB env source:', dbCfg.source);
  console.log('API env source:', apiCfg.source);

  // Test 1: single request sanity
  const first = await fetch(url, { method: 'HEAD', headers });
  console.log('Sanity HEAD status:', first.status);

  // Test 2: burst test for rate-limit behavior
  const burstCount = 15;
  const statuses = {};
  const start = Date.now();
  for (let i = 0; i < burstCount; i++) {
    const resp = await fetch(url, { method: 'HEAD', headers });
    statuses[resp.status] = (statuses[resp.status] || 0) + 1;
    const ra = resp.headers.get('retry-after');
    if (ra) {
      statuses['retry-after-header-seen'] = (statuses['retry-after-header-seen'] || 0) + 1;
    }
    await sleep(200);
  }
  const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);

  console.log('Burst requests:', burstCount, 'Elapsed(s):', elapsedSec);
  console.log('Status distribution:', JSON.stringify(statuses));

  // Test 3: paced test near configured limit (5 RPM => 12 sec gap)
  const pacedStatuses = {};
  const pacedCount = 3;
  for (let i = 0; i < pacedCount; i++) {
    const resp = await fetch(url, { method: 'HEAD', headers });
    pacedStatuses[resp.status] = (pacedStatuses[resp.status] || 0) + 1;
    if (i < pacedCount - 1) await sleep(12000);
  }
  console.log('Paced status distribution:', JSON.stringify(pacedStatuses));

  const report = {
    testedAt: new Date().toISOString(),
    endpoint: url,
    burstCount,
    burstElapsedSeconds: Number(elapsedSec),
    burstStatuses: statuses,
    pacedCount,
    pacedStatuses,
  };

  const out = path.join(root, 'diagnostics', 'files_endpoint_rate_test_report.json');
  fs.writeFileSync(out, JSON.stringify(report, null, 2), 'utf8');
  console.log('Report written:', out);
}

main().catch((e) => {
  console.error('Endpoint test failed:', e.message);
  process.exit(1);
});
