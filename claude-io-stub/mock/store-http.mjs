#!/usr/bin/env node
// Store RPC over plain HTTP (ws-line-v1 listener).
// usage: store-http.mjs BASE exists|messages|fork|delete ID [UPTO]
// Prints the response body verbatim; exits 1 only on server errors (5xx).

const [base, op, id, upto] = process.argv.slice(2);
const urls = {
  exists: `${base}/v1/store/sessions/${id}`,
  messages: `${base}/v1/store/sessions/${id}/messages`,
  fork: `${base}/v1/store/sessions/${id}/fork`,
  delete: `${base}/v1/store/sessions/${id}`,
};
if (!urls[op]) {
  console.error(`unknown op ${op}`);
  process.exit(1);
}
const init =
  op === "fork"
    ? { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ upTo: upto }) }
    : op === "delete"
      ? { method: "DELETE" }
      : {};
const res = await fetch(urls[op], init);
process.stdout.write(await res.text());
if (res.status >= 500) process.exit(1);
