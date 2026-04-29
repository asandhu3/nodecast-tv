const express = require('express');
const router = express.Router();
const { spawn } = require('child_process');

/**
 * Probe endpoint - detects stream codecs and container
 * GET /api/probe?url=...
 *
 * Successful response:
 *   { status: 'ok', video, audio, width, height, audioChannels, container,
 *     compatible, needsRemux, needsTranscode, subtitles }
 *
 * Failure response:
 *   { status: 'unavailable', reason: 'channel_offline'|'rate_limited'|'timeout'|'error',
 *     error?: <ffprobe stderr> }
 *
 * Successful probes are cached for 24h. Failures are NOT cached: every play attempt
 * of a previously-failing channel re-probes (so a recovered channel is reachable
 * immediately). Concurrent probes for the same URL are deduplicated to a single
 * ffprobe spawn via an in-flight Map.
 */

// Probe cache (URL → success result only). 24h since codecs don't change.
const probeCache = new Map();
const SUCCESS_TTL = 24 * 60 * 60 * 1000;

// In-flight probe dedup: Map<cacheKey, Promise>
const inFlightProbes = new Map();

// Per-attempt timeouts and inter-attempt delays.
// Worst case (3 timeouts + 2 gaps): 5 + 3 + 15 + 5 + 25 = 53s.
// Most healthy probes resolve on attempt 1 in <2s.
const PROBE_TIMEOUTS = [5000, 15000, 25000];
const RETRY_DELAYS = [3000, 5000];

// Browser-compatible codecs
const BROWSER_VIDEO_CODECS = ['h264', 'avc', 'avc1'];
const BROWSER_AUDIO_CODECS = ['aac', 'mp3', 'opus', 'vorbis'];

/**
 * Probe stream with ffprobe. Caller passes the per-attempt timeout.
 */
function probeStream(url, ffprobePath, userAgent, timeout) {
    return new Promise((resolve, reject) => {
        const args = [
            '-v', 'error',
            '-user_agent', userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            '-http_persistent', '0',
            '-reconnect', '1',
            '-reconnect_streamed', '1',
            '-reconnect_on_http_error', '4xx,5xx',
            '-reconnect_delay_max', '10',
            '-print_format', 'json',
            '-show_streams',
            '-show_format',
            '-probesize', '5000000',
            '-analyzeduration', '5000000',
            url
        ];

        const proc = spawn(ffprobePath, args);
        let stdout = '';
        let stderr = '';
        let timedOut = false;

        const timer = setTimeout(() => {
            timedOut = true;
            proc.kill('SIGKILL');
        }, timeout);

        proc.stdout.on('data', (data) => { stdout += data; });
        proc.stderr.on('data', (data) => { stderr += data; });

        proc.on('close', (code) => {
            clearTimeout(timer);
            if (timedOut) {
                const err = new Error('Probe timeout');
                err.timedOut = true;
                reject(err);
                return;
            }
            if (code !== 0) {
                const err = new Error(`ffprobe exited with code ${code}: ${stderr}`);
                err.stderr = stderr;
                err.exitCode = code;
                reject(err);
                return;
            }
            try {
                resolve(JSON.parse(stdout));
            } catch (e) {
                reject(new Error('Failed to parse ffprobe output'));
            }
        });

        proc.on('error', (err) => {
            clearTimeout(timer);
            reject(err);
        });
    });
}

/**
 * Classify a probe failure into one of: channel_offline, rate_limited, timeout, error.
 *
 * Heuristics drawn from observed ffprobe behaviour against Cloudflare-fronted
 * Xtream providers:
 *
 *   - "Stream ends prematurely at <N>" with small N (<500): the provider returned
 *     a tiny body in place of a manifest. We've seen this as e.g. a literal
 *     "Cannot read /home/nxt/storage/streams/<id>_.m3u8" filesystem error wrapped
 *     in 200 OK. Channel is offline at the source; retrying won't help.
 *
 *   - "Server returned 4XX Client Error, but not one of 40{0,1,3,4}": the CDN
 *     returned a non-standard 4xx (407/429/etc.) used as a rate-limit signal.
 *
 *   - SIGKILL'd by per-attempt timeout: timed out without other diagnostic info.
 *
 *   - Anything else: generic error (DNS, TCP, TLS, malformed JSON, etc.).
 */
function classifyFailure(err) {
    if (err && err.timedOut) return 'timeout';

    const msg = (err && (err.stderr || err.message)) || '';

    const premature = msg.match(/Stream ends prematurely at (\d+)/);
    if (premature && parseInt(premature[1], 10) < 500) {
        return 'channel_offline';
    }

    if (msg.includes('Server returned 4XX') ||
        msg.includes('Server returned 5XX') ||
        /HTTP error (407|408|409|410|429|451)/.test(msg)) {
        return 'rate_limited';
    }

    return 'error';
}

/**
 * Drive probeStream() through the retry tiers. Resolves either to
 *   { ok: true,  result: <parsed ffprobe JSON> } or
 *   { ok: false, reason: <category>, error: <last stderr/message> }
 */
async function probeWithRetries(url, ffprobePath, userAgent) {
    let lastReason = 'error';
    let lastErrorMsg = '';

    for (let i = 0; i < PROBE_TIMEOUTS.length; i++) {
        const timeout = PROBE_TIMEOUTS[i];
        try {
            const result = await probeStream(url, ffprobePath, userAgent, timeout);
            return { ok: true, result };
        } catch (err) {
            lastReason = classifyFailure(err);
            lastErrorMsg = (err.stderr || err.message || '').trim().slice(0, 500);
            console.log(`[Probe] Attempt ${i + 1}/${PROBE_TIMEOUTS.length} failed after ${timeout}ms: ${lastReason} - ${lastErrorMsg.split('\n')[0]}`);

            // No point retrying a stream the provider has pulled.
            if (lastReason === 'channel_offline') break;

            const isLastAttempt = i === PROBE_TIMEOUTS.length - 1;
            if (!isLastAttempt) {
                await new Promise(r => setTimeout(r, RETRY_DELAYS[i]));
            }
        }
    }

    return { ok: false, reason: lastReason, error: lastErrorMsg };
}

/**
 * Analyze probe result and determine compatibility
 */
function analyzeProbeResult(probeResult, url) {
    const streams = probeResult.streams || [];
    const format = probeResult.format || {};

    const videoStream = streams.find(s => s.codec_type === 'video');
    const audioStream = streams.find(s => s.codec_type === 'audio');

    const videoCodec = videoStream?.codec_name?.toLowerCase() || 'unknown';
    const audioCodec = audioStream?.codec_name?.toLowerCase() || 'unknown';
    const container = format.format_name?.toLowerCase() || 'unknown';

    // Check codec compatibility
    const videoOk = BROWSER_VIDEO_CODECS.some(c => videoCodec.includes(c));
    const audioOk = BROWSER_AUDIO_CODECS.some(c => audioCodec.includes(c));

    // Browser-safe containers
    // Note: We exclude 'webm' because ffprobe reports MKV as "matroska,webm",
    // and H.264/AAC in MKV/WebM is not universally supported. Best to remux to MP4.
    const BROWSER_CONTAINERS = ['hls', 'mp4', 'mov'];
    const containerOk = BROWSER_CONTAINERS.some(c => container.includes(c));

    // Check if it's a raw TS stream (not HLS)
    const isRawTs = (container.includes('mpegts') || url.endsWith('.ts')) && !url.includes('.m3u8');

    // Extract subtitle tracks
    const subtitles = streams
        .filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'timed_id3' && s.codec_name !== 'bin_data')
        .map(s => ({
            index: s.index,
            language: s.tags?.language || 'und',
            title: s.tags?.title || s.tags?.language || `Track ${s.index}`,
            codec: s.codec_name
        }));

    // Determine what processing is needed
    // MKV files often cause OOM/decoding issues in browser fMP4 remux,
    // so we force them to "needsTranscode" which uses HLS (more robust).
    // The frontend will still use "copy" mode if codecs are compatible.
    const isMkv = container.includes('matroska') || container.includes('webm') || url.endsWith('.mkv');

    // 1. Incompatible audio/video OR MKV -> Transcode (or HLS Copy)
    const needsTranscode = !audioOk || !videoOk || isMkv;

    // 2. Compatible audio/video but incompatible container (non-MKV) -> Remux (fMP4 pipe)
    const needsRemux = !needsTranscode && (!containerOk || isRawTs);

    const compatible = !needsTranscode && !needsRemux;

    return {
        video: videoCodec,
        audio: audioCodec,
        width: videoStream?.width || 0,
        height: videoStream?.height || 0,
        audioChannels: audioStream?.channels || 0, // For Smart Audio Copy
        container: container,
        compatible: compatible,
        needsRemux: needsRemux,
        needsTranscode: needsTranscode,
        subtitles: subtitles
    };
}

router.get('/', async (req, res) => {
    const { url, ua } = req.query;
    if (!url) {
        return res.status(400).json({ error: 'URL parameter is required' });
    }

    const ffprobePath = req.app.locals.ffprobePath;
    const cacheKey = `${url}${ua ? `|${ua}` : ''}`;

    if (!ffprobePath) {
        // No ffprobe binary at all — we can't determine, so let the frontend try direct play.
        // (Different from probe-ran-and-failed; that returns status:unavailable.)
        console.log('[Probe] FFprobe not available, returning permissive default');
        return res.json({
            status: 'ok',
            video: 'unknown',
            audio: 'unknown',
            container: 'unknown',
            width: 0,
            height: 0,
            audioChannels: 0,
            compatible: true,
            needsRemux: false,
            needsTranscode: false,
            subtitles: []
        });
    }

    // Cache hit (success only — failures are not cached)
    const cached = probeCache.get(cacheKey);
    if (cached && (Date.now() - cached.timestamp < SUCCESS_TTL)) {
        console.log(`[Probe] Cache hit for: ${url.substring(0, 50)}...`);
        return res.json(cached.result);
    }

    // In-flight dedup: if another request is already probing this URL, await it.
    const existing = inFlightProbes.get(cacheKey);
    if (existing) {
        console.log(`[Probe] Joining in-flight probe for: ${url.substring(0, 50)}...`);
        try {
            const result = await existing;
            return res.json(result);
        } catch (err) {
            // Shouldn't happen — probePromise below catches everything — but be defensive.
            return res.json({ status: 'unavailable', reason: 'error', error: err.message });
        }
    }

    console.log(`[Probe] Probing: ${url.substring(0, 80)}... ${ua ? `(UA: ${ua})` : ''}`);

    const probePromise = (async () => {
        const outcome = await probeWithRetries(url, ffprobePath, ua);

        if (outcome.ok) {
            const analysis = analyzeProbeResult(outcome.result, url);
            const response = { status: 'ok', ...analysis };

            probeCache.set(cacheKey, { result: response, timestamp: Date.now() });

            console.log(`[Probe] Result: video=${analysis.video}, audio=${analysis.audio}, ` +
                `${analysis.width}x${analysis.height}, container=${analysis.container}, ` +
                `compatible=${analysis.compatible}, needsRemux=${analysis.needsRemux}, ` +
                `needsTranscode=${analysis.needsTranscode}`);

            return response;
        }

        console.log(`[Probe] Unavailable: reason=${outcome.reason}`);
        return {
            status: 'unavailable',
            reason: outcome.reason,
            error: outcome.error
        };
    })();

    inFlightProbes.set(cacheKey, probePromise);

    try {
        const result = await probePromise;
        res.json(result);
    } finally {
        inFlightProbes.delete(cacheKey);
    }
});

module.exports = router;
