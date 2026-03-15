//! Signal Protocol load test.
//!
//! Scenarios:
//!   x3dh         — X3DH key exchange throughput (initiate + respond)
//!   ratchet      — Double Ratchet encrypt/decrypt throughput (1:1)
//!   ratchet_ser  — RatchetSession CBOR serialization throughput
//!   group        — Sender Keys group encrypt/decrypt throughput
//!
//! Usage:
//!   cargo run -p yata-bench --bin signal-bench --release -- [all|x3dh|ratchet|ratchet_ser|group]

use std::time::{Duration, Instant};
use anyhow::Result;
use yata_signal::{
    DHKeyPair, IdentityKeyPair, OneTimePreKey, PreKeyBundle, SignedPreKey,
    GroupSession, RatchetSession,
    x3dh_initiate, x3dh_respond,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.min(sorted.len()) - 1]
}

fn print_latency(label: &str, samples: &mut Vec<u64>) {
    samples.sort_unstable();
    let n = samples.len();
    let mean = samples.iter().sum::<u64>() / n as u64;
    println!(
        "  {:<32} n={:>5}  mean={:>6}µs  p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs",
        label, n, mean,
        percentile(samples, 50.0), percentile(samples, 95.0),
        percentile(samples, 99.0), samples.last().copied().unwrap_or(0),
    );
}

fn print_latency_ns(label: &str, samples: &mut Vec<u64>) {
    samples.sort_unstable();
    let n = samples.len();
    let mean = samples.iter().sum::<u64>() / n as u64;
    println!(
        "  {:<32} n={:>5}  mean={:>5}ns  p50={:>5}ns  p95={:>5}ns  p99={:>5}ns",
        label, n, mean,
        percentile(samples, 50.0), percentile(samples, 95.0), percentile(samples, 99.0),
    );
}

fn print_throughput(label: &str, count: u64, elapsed: Duration) {
    let ops_sec = count as f64 / elapsed.as_secs_f64();
    println!("  {:<32} {:.0} ops/sec  ({} ops in {:.2}s)", label, ops_sec, count, elapsed.as_secs_f64());
}

// ── key helpers ───────────────────────────────────────────────────────────────

fn make_bundle(ik: &IdentityKeyPair) -> (SignedPreKey, OneTimePreKey, PreKeyBundle) {
    let spk = SignedPreKey::generate(ik, 1);
    let opk = OneTimePreKey::generate(1);
    let bundle = PreKeyBundle {
        registration_id: ik.registration_id(),
        identity_key:      ik.dh_public_bytes(),
        identity_sign_key: ik.sign_public_bytes(),
        spk_id:            spk.key_id,
        spk_public:        spk.key_pair.public_bytes(),
        spk_signature:     spk.signature,
        opk_id:            Some(opk.key_id),
        opk_public:        Some(opk.key_pair.public_bytes()),
    };
    (spk, opk, bundle)
}

fn make_session_pair() -> (RatchetSession, RatchetSession) {
    let alice_ik = IdentityKeyPair::generate();
    let bob_ik   = IdentityKeyPair::generate();
    let (spk, opk, bundle) = make_bundle(&bob_ik);

    let (alice_ss, init_msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
    let bob_ss = x3dh_respond(&bob_ik, &spk, Some(&opk), &init_msg).unwrap();

    let alice = RatchetSession::init_sender(&alice_ss, bundle.spk_public).unwrap();
    let bob   = RatchetSession::init_receiver(&bob_ss,
        x25519_dalek::StaticSecret::from(spk.key_pair.secret_bytes())).unwrap();
    (alice, bob)
}

// ── scenario 1: X3DH key exchange ────────────────────────────────────────────

fn bench_x3dh() -> Result<()> {
    println!("\n[Signal Protocol — X3DH key exchange]");

    // Pre-generate Bob's key material (bundle reuse is realistic: server caches it)
    let bob_ik = IdentityKeyPair::generate();
    let (spk, opk, bundle) = make_bundle(&bob_ik);

    const WARMUP: usize = 200;
    const ITERS: usize  = 2_000;

    // --- initiate (Alice side: 3 DH + 1 HKDF) ---
    let alice_ik = IdentityKeyPair::generate();
    for _ in 0..WARMUP {
        let _ = x3dh_initiate(&alice_ik, &bundle).unwrap();
    }
    let mut init_samples = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = x3dh_initiate(&alice_ik, &bundle).unwrap();
        init_samples.push(t.elapsed().as_micros() as u64);
    }
    print_throughput("x3dh_initiate", ITERS as u64, start.elapsed());
    print_latency("x3dh_initiate latency", &mut init_samples);

    // --- respond (Bob side: 3 DH + 1 HKDF) ---
    let (_, init_msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
    let mut resp_samples = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = x3dh_respond(&bob_ik, &spk, Some(&opk), &init_msg).unwrap();
        resp_samples.push(t.elapsed().as_micros() as u64);
    }
    print_throughput("x3dh_respond", ITERS as u64, start.elapsed());
    print_latency("x3dh_respond latency", &mut resp_samples);

    // --- full round-trip (initiate + respond) ---
    let mut rt_samples = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        let (_, msg) = x3dh_initiate(&alice_ik, &bundle).unwrap();
        let _ = x3dh_respond(&bob_ik, &spk, Some(&opk), &msg).unwrap();
        rt_samples.push(t.elapsed().as_micros() as u64);
    }
    print_throughput("x3dh round-trip", ITERS as u64, start.elapsed());
    print_latency("x3dh round-trip latency", &mut rt_samples);

    Ok(())
}

// ── scenario 2: Double Ratchet encrypt/decrypt ────────────────────────────────

fn bench_ratchet() -> Result<()> {
    println!("\n[Signal Protocol — Double Ratchet 1:1 encrypt/decrypt]");

    let msg_sizes = [64usize, 256, 1024, 4096];

    for &size in &msg_sizes {
        let plaintext = vec![0x42u8; size];
        const ITERS: usize = 5_000;

        // Encrypt (Alice → Bob)
        {
            let (mut alice, _) = make_session_pair();
            // warmup
            for _ in 0..100 {
                let _ = alice.encrypt(&plaintext).unwrap();
            }
            let (mut alice2, _) = make_session_pair();
            let mut enc_samples = Vec::with_capacity(ITERS);
            let start = Instant::now();
            for _ in 0..ITERS {
                let t = Instant::now();
                let _ = alice2.encrypt(&plaintext).unwrap();
                enc_samples.push(t.elapsed().as_nanos() as u64);
            }
            let elapsed = start.elapsed();
            print_throughput(&format!("encrypt {}B", size), ITERS as u64, elapsed);
            print_latency_ns(&format!("  encrypt {}B latency", size), &mut enc_samples);
        }

        // Decrypt (Bob receives from Alice)
        {
            let (mut alice, mut bob) = make_session_pair();
            // pre-encrypt messages for bob to decrypt
            let messages: Vec<_> = (0..ITERS).map(|_| alice.encrypt(&plaintext).unwrap()).collect();
            let mut dec_samples = Vec::with_capacity(ITERS);
            let start = Instant::now();
            for msg in &messages {
                let t = Instant::now();
                let _ = bob.decrypt(msg).unwrap();
                dec_samples.push(t.elapsed().as_nanos() as u64);
            }
            let elapsed = start.elapsed();
            print_throughput(&format!("decrypt {}B", size), ITERS as u64, elapsed);
            print_latency_ns(&format!("  decrypt {}B latency", size), &mut dec_samples);
        }
    }

    // Bidirectional (Alice↔Bob alternating, triggers DH ratchet steps)
    {
        const ITERS: usize = 1_000;
        let plaintext = vec![0x42u8; 256];
        let (mut alice, mut bob) = make_session_pair();
        let mut bidir_samples = Vec::with_capacity(ITERS * 2);
        let start = Instant::now();
        for _ in 0..ITERS {
            let t = Instant::now();
            let enc = alice.encrypt(&plaintext).unwrap();
            let _ = bob.decrypt(&enc).unwrap();
            bidir_samples.push(t.elapsed().as_nanos() as u64);

            let t = Instant::now();
            let enc = bob.encrypt(&plaintext).unwrap();
            let _ = alice.decrypt(&enc).unwrap();
            bidir_samples.push(t.elapsed().as_nanos() as u64);
        }
        print_throughput("bidirectional (256B)", (ITERS * 2) as u64, start.elapsed());
        print_latency_ns("  bidir enc+dec latency", &mut bidir_samples);
    }

    Ok(())
}

// ── scenario 3: RatchetSession serialization ─────────────────────────────────

fn bench_ratchet_serialization() -> Result<()> {
    println!("\n[Signal Protocol — RatchetSession CBOR serialization]");

    let (mut alice, _) = make_session_pair();
    // Advance ratchet to have some skipped keys in state
    let plaintext = vec![0u8; 64];
    for _ in 0..50 { let _ = alice.encrypt(&plaintext); }

    const ITERS: usize = 5_000;

    // Serialize
    let mut ser_samples = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = alice.to_cbor().unwrap();
        ser_samples.push(t.elapsed().as_nanos() as u64);
    }
    print_throughput("to_cbor (serialize)", ITERS as u64, start.elapsed());
    print_latency_ns("  to_cbor latency", &mut ser_samples);

    // Deserialize
    let cbor = alice.to_cbor().unwrap();
    let mut deser_samples = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = RatchetSession::from_cbor(&cbor).unwrap();
        deser_samples.push(t.elapsed().as_nanos() as u64);
    }
    print_throughput("from_cbor (deserialize)", ITERS as u64, start.elapsed());
    print_latency_ns("  from_cbor latency", &mut deser_samples);

    println!("  session CBOR size: {} bytes", cbor.len());
    Ok(())
}

// ── scenario 4: Sender Keys group encrypt/decrypt ─────────────────────────────

fn bench_group() -> Result<()> {
    println!("\n[Signal Protocol — Sender Keys group encrypt/decrypt]");

    let group_sizes = [2usize, 10, 50];
    let msg_sizes   = [64usize, 256, 1024];

    for &n_members in &group_sizes {
        for &msg_size in &msg_sizes {
            let plaintext = vec![0x42u8; msg_size];
            const ITERS: usize = 5_000;
            let sender_did = "did:plc:sender";

            // Encrypt bench: independent sender session (no need to share dist with receivers)
            if msg_size == 256 {
                let mut enc_sender = GroupSession::new("group-bench-enc", sender_did);
                let _ = enc_sender.init_sender().unwrap();
                let mut enc_samples = Vec::with_capacity(ITERS);
                let start = Instant::now();
                for _ in 0..ITERS {
                    let t = Instant::now();
                    let _ = enc_sender.encrypt(&plaintext).unwrap();
                    enc_samples.push(t.elapsed().as_nanos() as u64);
                }
                let elapsed = start.elapsed();
                print_throughput(&format!("group encrypt {}B ({}m)", msg_size, n_members), ITERS as u64, elapsed);
                print_latency_ns(&format!("  group enc {}B latency", msg_size), &mut enc_samples);
            }

            // Decrypt bench: sender produces messages, receiver decrypts
            {
                let mut dec_sender = GroupSession::new("group-bench-dec", sender_did);
                let dist = dec_sender.init_sender().unwrap();
                let mut receivers: Vec<GroupSession> = (0..n_members).map(|i| {
                    let mut s = GroupSession::new("group-bench-dec", format!("did:plc:member{}", i));
                    s.process_distribution(&dist).unwrap();
                    s
                }).collect();

                let messages: Vec<_> = (0..ITERS).map(|_| dec_sender.encrypt(&plaintext).unwrap()).collect();
                let receiver = &mut receivers[0];
                let mut dec_samples = Vec::with_capacity(ITERS);
                let start = Instant::now();
                for msg in &messages {
                    let t = Instant::now();
                    let _ = receiver.decrypt(msg).unwrap();
                    dec_samples.push(t.elapsed().as_nanos() as u64);
                }
                let elapsed = start.elapsed();
                if msg_size == 256 {
                    print_throughput(&format!("group decrypt {}B ({}m)", msg_size, n_members), ITERS as u64, elapsed);
                    print_latency_ns(&format!("  group dec {}B latency", msg_size), &mut dec_samples);
                }
            }
        }
    }

    // Broadcast: sender encrypts once, n receivers each decrypt
    {
        const MSGS: usize  = 1_000;
        const MEMBERS: usize = 50;
        let plaintext = vec![0x42u8; 256];

        let sender_did = "did:plc:bcast-sender";
        let mut sender = GroupSession::new("group-bcast", sender_did);
        let dist = sender.init_sender().unwrap();
        let mut receivers: Vec<GroupSession> = (0..MEMBERS).map(|i| {
            let mut s = GroupSession::new("group-bcast", format!("did:plc:bm{}", i));
            s.process_distribution(&dist).unwrap();
            s
        }).collect();

        let messages: Vec<_> = (0..MSGS).map(|_| sender.encrypt(&plaintext).unwrap()).collect();
        let start = Instant::now();
        let mut total_decrypts = 0u64;
        for msg in &messages {
            for r in &mut receivers {
                let _ = r.decrypt(msg).unwrap();
                total_decrypts += 1;
            }
        }
        let elapsed = start.elapsed();
        println!();
        print_throughput(&format!("broadcast decrypt {} msgs × {}m", MSGS, MEMBERS), total_decrypts, elapsed);
    }

    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║           Signal Protocol Load Test                      ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("scenario: {}", scenario);

    let run = |name: &str| -> bool { scenario == "all" || scenario == name };

    if run("x3dh")        { bench_x3dh()?; }
    if run("ratchet")     { bench_ratchet()?; }
    if run("ratchet_ser") { bench_ratchet_serialization()?; }
    if run("group")       { bench_group()?; }

    println!("\ndone");
    Ok(())
}
