use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tempfile::TempDir;

pub fn bench(c: &mut Criterion) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut rng = thread_rng();
    for _ in 0..100 {
        let (rand_key_len, rand_value_len) = (rng.gen_range(1, 1_000), rng.gen_range(1, 1_000));

        let rand_key: String = rng.sample_iter(&Alphanumeric).take(rand_key_len).collect();
        keys.push(rand_key);

        let rand_value: String = rng
            .sample_iter(&Alphanumeric)
            .take(rand_value_len)
            .collect();
        values.push(rand_value);
    }

    let mut group = c.benchmark_group("engines read write bench");

    let (kvs_dir, sled_dir) = (TempDir::new().unwrap(), TempDir::new().unwrap());

    let kvs = KvStore::open(kvs_dir.path()).unwrap();
    let sled = SledKvsEngine {
        db: sled::open(sled_dir.path()).unwrap(),
    };

    group.bench_function("kvs write", |b| {
        b.iter(|| {
            keys.iter()
                .zip(values.iter())
                .for_each(|(k, v)| kvs.set(k.clone(), v.clone()).unwrap())
        })
    });

    group.bench_function("sled write", |b| {
        b.iter(|| {
            keys.iter()
                .zip(values.iter())
                .for_each(|(k, v)| sled.set(k.clone(), v.clone()).unwrap())
        })
    });

    group.bench_function("kvs read", |b| {
        b.iter(|| {
            keys.iter()
                .zip(values.iter())
                .for_each(|(k, v)| assert_eq!(kvs.get(k.clone()).unwrap().unwrap(), v.clone()))
        })
    });

    group.bench_function("sled read", |b| {
        b.iter(|| {
            keys.iter()
                .zip(values.iter())
                .for_each(|(k, v)| assert_eq!(sled.get(k.clone()).unwrap().unwrap(), v.clone()))
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
