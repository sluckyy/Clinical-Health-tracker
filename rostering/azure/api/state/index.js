// Shared roster state API for the on-call board.
// GET  /api/state -> { state, etag }   (state: null when nothing saved yet)
// PUT  /api/state  { state, etag } -> { ok, etag }
//   etag enforces optimistic concurrency: a stale save returns 409 and the
//   client reloads the latest board instead of clobbering someone's change.
const { BlobServiceClient } = require('@azure/storage-blob');

const CONTAINER = 'roster-state';
const BLOB = 'state.json';

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  return Buffer.concat(chunks).toString('utf8');
}

module.exports = async function (context, req) {
  const conn = process.env.STATE_STORAGE_CONNECTION || process.env.AzureWebJobsStorage;
  if (!conn) {
    context.res = { status: 500, body: { error: 'No storage configured. Add the STATE_STORAGE_CONNECTION application setting (a storage account connection string).' } };
    return;
  }
  const container = BlobServiceClient.fromConnectionString(conn).getContainerClient(CONTAINER);
  await container.createIfNotExists();
  const blob = container.getBlockBlobClient(BLOB);

  if (req.method === 'GET') {
    try {
      const dl = await blob.download();
      const text = await streamToString(dl.readableStreamBody);
      context.res = { headers: { 'content-type': 'application/json' }, body: { state: JSON.parse(text), etag: dl.etag } };
    } catch (e) {
      if (e.statusCode === 404) context.res = { body: { state: null, etag: null } };
      else throw e;
    }
    return;
  }

  // PUT
  const { state, etag } = req.body || {};
  if (!state) { context.res = { status: 400, body: { error: 'state required' } }; return; }
  const data = JSON.stringify(state);
  try {
    const conditions = etag ? { ifMatch: etag } : { ifNoneMatch: '*' };
    const r = await blob.upload(data, Buffer.byteLength(data), { conditions, blobHTTPHeaders: { blobContentType: 'application/json' } });
    context.res = { body: { ok: true, etag: r.etag } };
  } catch (e) {
    if (e.statusCode === 412 || e.statusCode === 409) context.res = { status: 409, body: { error: 'conflict' } };
    else throw e;
  }
};
