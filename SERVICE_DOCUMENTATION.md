# VP-FMS Interceptor Service Documentation

## 1. Ringkasan

Service ini adalah **TCP interceptor** di antara **VP** dan **FMS**:

- meneruskan trafik VP -> FMS secara transparan (hot path),
- menyalin frame untuk dipublish ke Kafka (side path),
- memastikan masalah Kafka tidak mengganggu jalur transaksi VP<->FMS.

```
VP ---> [ interceptor ] ---> FMS
             |
             +--> Kafka
```

## 2. Komponen Utama

- **Proxy path**: forward byte stream antar socket VP dan FMS.
- **Tee path**: copy data ke worker queue (bounded, non-blocking).
- **Framer**: reassemble frame (mode `length_prefix` atau `raw`).
- **Kafka publisher**: kirim frame ke topic Kafka.
- **Admin endpoint**: health dan metrics.

## 3. Konfigurasi Penting (`config.toml`)

### `[listen]`
- alamat service untuk menerima koneksi dari VP.

### `[upstream]`
- endpoint FMS tujuan.

### `[tee]`
- `shards`, `queue_capacity`, `publish_directions`.
- queue ini adalah batas isolasi agar Kafka tidak backpressure ke hot path.

### `[framing]`
- `mode = "length_prefix"` (umum untuk ISO8583) atau `raw`.
- pengaturan prefix (`prefix_bytes`, endianness, dll).

### `[kafka]`
- broker, topic, format payload (`json`/`raw`), encoding (`base64`/`hex`/`utf8`).

### `[kafka.filter]`
Filter untuk drop frame tertentu **hanya dari publish Kafka** (forward tetap jalan).

Contoh filter echo (0800/0810, DE70=301):

```toml
[kafka.filter]
any_of = [
  { all_of = [
      { mti = ["0800", "0810"] },
      { de70 = ["301"] }
    ] }
]
```

Semantik:
- `any_of`: OR antar grup.
- `all_of`: AND dalam grup.
- match exact string.
- jika frame gagal diparse untuk evaluasi filter, frame tetap dipublish.

## 4. Format Message Kafka

Default (`value_format = "json"`):

- metadata: `conn_id`, `direction`, `seq`, `peer`, `ts_ms`, dst.
- `payload`: isi frame asli dalam encoding yang dipilih (default base64).

Jika `value_format = "raw"`, body Kafka adalah byte frame mentah dan metadata ada di headers.

## 5. Operasional

- Health check: `/healthz`
- Metrics: `/metrics`
- Counter penting:
  - `tee_dropped`
  - `kafka_enqueue_failed`
  - `kafka_delivery_failed`
  - `framer_desyncs`

## 6. Build dan Run

### Rust
```bash
cd rust-interceptor
cargo test
cargo build --release
./target/release/vp-fms-interceptor ../config.toml
```

### Go
```bash
cd go-interceptor
go test ./...
go build -o bin/vp-fms-interceptor ./cmd/interceptor
./bin/vp-fms-interceptor ../config.toml
```

## 7. Catatan Penting

- Service ini menangani data sensitif (ISO8583), jadi kontrol akses topic Kafka wajib ketat.
- `base64` aman untuk payload binary; gunakan `hex` jika perlu lebih mudah dibaca saat troubleshooting.
- Filter Kafka tidak boleh dipakai untuk logic bisnis transaksi; hanya untuk observability stream.
