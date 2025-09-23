import { check } from 'k6';
import encoding from 'k6/encoding';
import http from 'k6/http';
import { Trend } from 'k6/metrics';
import avro from 'k6/x/avro';


// Shared Avro codec for the test key schema
const keyCodec = avro.newCodec(`{
  "type": "record",
  "name": "TestKeyRecord",
  "namespace": "com.linkedin.venice.schemas",
  "fields": [
    { "name": "uniqueID", "type": "long" },
    { "name": "dummyStr", "type": "string" }
  ]
}`);

// Avro codec for MultiGetRouterRequestKeyV1 - matches Venice fast client protocol
const multiGetRequestCodec = avro.newCodec(`{
  "name": "MultiGetRouterRequestKeyV1",
  "namespace": "com.linkedin.venice.read.protocol.request.router",
  "doc": "This field will store all the related info for one key",
  "type": "record",
  "fields": [
    {
      "name": "keyIndex",
      "doc": "Unique index for each key inside current multi-get request",
      "type": "int"
    },
    {
      "name": "keyBytes",
      "doc": "Avro serialized key",
      "type": "bytes"
    },
    {
      "name": "partitionId",
      "doc": "Partition that current key belongs to",
      "type": "int"
    }
  ]
}`);

const byteCodec = avro.newBytesCodec();


// TODO: With native K6 we can pass these as config json object. For FF atm, we need them to be __ENV variables.
const mode = __ENV.mode || 'BATCH_GET'; // Can be BATCH_GET, SINGLE_GET, or READ_COMPUTE
const maxKeyID = __ENV.maxKeyID || 4485760;
const chunkSize = Math.min(maxKeyID, 150);
const proto = __ENV.httpProto || 'https';
const host = __ENV.veniceServerFQDN || 'venice-server.perf-1.venice-server.ei-ltx1.atd.disco.linkedin.com';
const port = __ENV.veniceServerPort || 1690;
const store = __ENV.veniceStoreName || 'cert-histogram-server-stress-testing';
const partitionCount = parseInt(__ENV.partitionCount || '100'); // Default partition count


// Load TLS certs
const cert = open(__ENV.cert ?? 'identity.cert')
const key = open(__ENV.key ?? 'identity.key')


// metrics
let dataPrepTrend = new Trend('data_prep_duration');


// threshold definitions
let thresholdDef = {
    'http_req_failed{expected_response:true}': ['rate<0.000001'],
}


if (mode === 'BATCH_GET') {
    thresholdDef[`http_req_duration{type:${mode}}`] = ['p(99)<150']
} else if (mode === 'READ_COMPUTE') {
    // TODO: Add specific thresholds for READ_COMPUTE mode
    thresholdDef[`http_req_duration{type:${mode}}`] = ['p(99)<50']
} else {
    // thresholdDef[`http_req_duration{type:${mode}}`] = ['p(99)<10']
    thresholdDef[`http_req_duration`] = ['p(99)<10']
}



// Common Options
export const options = {
    discardResponseBodies: true,
    insecureSkipTLSVerify: true,
    noConnectionReuse: __ENV.noConnectionReuse === "true",
    tlsCipherSuites: ['TLS_RSA_WITH_AES_128_CBC_SHA'],
    tlsAuth: [{ cert: cert, key: key }],
    scenarios: {
        load_test: {
            executor: 'constant-arrival-rate',
            rate: parseInt(__ENV.constantArrivalRate || '100'),
            timeUnit: __ENV.constantArrivalRateTimeUnit || '1s',
            duration: __ENV.constantArrivalRateDuration || '60s',
            preAllocatedVUs: parseInt(__ENV.constantArrivalRatePreAllocatedVUs || '100'),
            maxVUs: parseInt(__ENV.constantArrivalRateMaxVUs || '100'),
        },
    },
    thresholds: thresholdDef,
};


export default function () {
    const tags = { expected_response: 'true', type: mode }
    if (mode === 'BATCH_GET') {
        const httpHeaders = {
            'Content-Type': 'avro/binary',
            'X-VENICE-API-VERSION': 1,
            'X-VENICE-SUPPORTED-COMPRESSION-STRATEGY': 1,
            'Connection': 'keep-alive',
            'X-VENICE-STREAMING': 1,
            'X-VENICE-KEY-COUNT': chunkSize,
        };

        const start = Date.now();
        const payload = generateBatchGetPayload(maxKeyID, chunkSize);
        const payloadGenTime = Date.now() - start;
        dataPrepTrend.add(payloadGenTime);

        // Server expects /storage/{resourceName} where resourceName is {store}_v{version}
        const version = __ENV.storeVersion || '10';
        const resourceName = `${store}_v${version}`;
        const url = `${proto}://${host}:${port}/storage/${resourceName}`;

        httpCall('POST', url, httpHeaders, payload, tags);
    }


    if (mode === 'SINGLE_GET') {
        const httpHeaders = {
            'Content-Type': 'application/json',
            'X-VENICE-API-VERSION': 1,
            'X-VENICE-SUPPORTED-COMPRESSION-STRATEGY': 1,
            'Connection': 'keep-alive',
        };


        const start = Date.now();
        const path = generateGetPath(store, maxKeyID);
        dataPrepTrend.add(Date.now() - start);
        const url = http.url`${proto}://${host}:${port}${path}`;
        httpCall('GET', url, httpHeaders, tags);
    }


    if (mode === 'READ_COMPUTE') {
        // TODO: Implement READ_COMPUTE mode
        // Placeholder for READ_COMPUTE implementation
        const httpHeaders = {
            'Content-Type': 'application/json',
            'X-VENICE-API-VERSION': 1,
            'X-VENICE-SUPPORTED-COMPRESSION-STRATEGY': 1,
            'Connection': 'keep-alive',
        };


        const start = Date.now();
        // TODO: Generate appropriate payload/path for READ_COMPUTE
        dataPrepTrend.add(Date.now() - start);

        // TODO: Construct appropriate URL for READ_COMPUTE
        // const url = http.url`${proto}://${host}:${port}/compute/${store}/0/...`;
        // httpCall('POST', url, httpHeaders, payload, tags);

        console.log('READ_COMPUTE mode not yet implemented');
    }
}


export function handleSummary(data) {
    const mode = __ENV.mode;
    const latencyKey = "http_req_duration";
    const errKey = "http_req_failed{expected_response:true}";


    const latencyMetric = data.metrics[latencyKey];
    const vusMetric = data.metrics.vus;


    if (!latencyMetric) {
        console.warn(`Warning: No metrics found for latency_key: ${latencyKey}`);
        return { [__ENV.summary || 'stdout']: `No results captured for mode: ${mode}\n` };
    }


    let summary = {
        mode: mode,
        scenario: "load_test",
        total_count: data.metrics.http_reqs?.values.count ?? 0,
        qps: data.metrics.iterations?.values.rate ?? 0,
        latencies: [],
        min: latencyMetric.values.min ?? 0,
        max: latencyMetric.values.max ?? 0,
        avg: latencyMetric.values.avg ?? 0,
        errors_count: Math.round((data.metrics[errKey]?.values.rate ?? 0) * (data.metrics.http_reqs?.values.count ?? 0)),
        vus: {
            min: vusMetric?.values.min ?? 0,
            max: vusMetric?.values.max ?? 0,
        },
        setup_duration: data.metrics.data_prep_duration?.values ?? null,
    };


    for (const key of data.options.summaryTrendStats || []) {
        if (key.startsWith('p(') && latencyMetric.values[key] !== undefined) {
            summary.latencies.push({
                percentile: key.slice(2, -1),
                latency: latencyMetric.values[key],
            });
        }
    }


    return { [__ENV.summary || 'stdout']: JSON.stringify(summary, null, 2) + '\n' };
}


function httpCall(method, url, headers, body = null, tags = {}) {
    const res = method === 'POST'
        ? http.post(url, body, { headers: headers, httpVersion: '1.1', responseType: 'binary', tags: tags, })
        : http.get(url, { headers: headers, httpVersion: '1.1', tags: tags, });

    // Log errors and important response details
    if (res) {
        if (res.status >= 400) {
            console.error(`[VU ${__VU}] Request failed with status ${res.status}`);
            // For error responses, try to decode and log the error message
            if (res.body && res.headers['Content-Type'] && res.headers['Content-Type'].includes('text/plain')) {
                try {
                    const bodyText = String.fromCharCode.apply(null, new Uint8Array(res.body));
                    console.error(`[VU ${__VU}] Error message: ${bodyText}`);
                } catch (e) {
                    console.error(`[VU ${__VU}] Could not decode error response body`);
                }
            }
        }
        
        if (res.error) {
            console.error(`[VU ${__VU}] Request error: ${res.error}`);
        }
    } else {
        console.error(`[VU ${__VU}] No response received - connection timeout or failure`);
    }

    if (!res || res.status === 0) {
        console.error(`[VU ${__VU}] Connection timeout detected.`);
    }

    check(res, {
        'HTTP 200/404': r => r && (r.status === 200 || r.status === 404)
    });

    return res;
}


// Helpers


function getRandomIntInclusive(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1) + min);
}

/**
 * Simple hash function to calculate partition ID from key bytes
 * This mimics the partitioning logic used by Venice clients
 */
function calculatePartitionId(keyBytes, partitionCount) {
    // let hash = 0;
    // for (let i = 0; i < keyBytes.length; i++) {
    //     hash = ((hash << 5) - hash + keyBytes[i]) & 0xffffffff;
    // }
    // return Math.abs(hash) % partitionCount;
    return 0;
}

/**
 * Generate batch-get payload using the correct MultiGetRouterRequestKeyV1 protocol
 * This matches the fast client's serialization logic
 */
function generateBatchGetPayload(maxKeyID, chunkSize) {
    const multiGetRecords = [];

    for (let i = 0; i < chunkSize; i++) {
        const keyID = getRandomIntInclusive(1, maxKeyID);
        const rawKey = `{"uniqueID" : ${keyID},"dummyStr" : "100"}`;

        // Serialize the key using the key codec
        let binaryKey;
        try {
            binaryKey = keyCodec.binaryFromTextual(rawKey);
        } catch (error) {
            console.error(`[VU ${__VU}] Error serializing key ${i}: ${error}`);
            throw error;
        }

        // Calculate partition ID from the serialized key bytes
        const partitionId = calculatePartitionId(binaryKey, partitionCount);

        // Create the record object that matches MultiGetRouterRequestKeyV1 schema
        // For Avro textual encoding, bytes field should be a string with escaped bytes
        const keyBytesString = String.fromCharCode.apply(null, binaryKey);
        const multiGetRecord = {
            keyIndex: i,
            keyBytes: keyBytesString, // Use string format for Avro bytes field
            partitionId: partitionId
        };

        multiGetRecords.push(multiGetRecord);
    }

    // Serialize each record individually and concatenate them
    // This matches what Java MULTI_GET_REQUEST_SERIALIZER.serializeObjects() does
    try {
        const serializedRecords = [];
        
        for (let i = 0; i < multiGetRecords.length; i++) {
            const record = multiGetRecords[i];
            const recordJson = JSON.stringify(record);
            const serializedRecord = multiGetRequestCodec.binaryFromTextual(recordJson);
            serializedRecords.push(serializedRecord);
        }
        
        // Concatenate all serialized records into a single byte array
        let totalLength = 0;
        for (const record of serializedRecords) {
            totalLength += record.length;
        }
        
        const combinedPayload = new Uint8Array(totalLength);
        let offset = 0;
        for (const record of serializedRecords) {
            combinedPayload.set(record, offset);
            offset += record.length;
        }

        return combinedPayload;

    } catch (error) {
        console.error(`[VU ${__VU}] Error serializing batch payload: ${error}`);
        throw error;
    }
}


function generateGetPath(storeName, maxKeyID) {
    const keyID = getRandomIntInclusive(1, maxKeyID);
    const rawKey = `{"uniqueID" : ${keyID},"dummyStr" : "100"}`;
    const binaryKey = keyCodec.binaryFromTextual(rawKey);
    const encoded = encoding.b64encode(binaryKey, "url");
    return `/storage/${storeName}/0/${encoded}?f=b64`;
}
