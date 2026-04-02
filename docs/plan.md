Python apps untuk data tools v1

Tujuan:

- Menerima URL Google Drive dari user dan memproses data menjadi output terstandar.
- Mendukung modular per model (daftar model ada di model/model.csv).

Mode Flow (ringkas):

- Deteksi jenis file dari URL (Google Spreadsheet atau file .xlsx).
- Unduh data menjadi file Excel lokal.
- Jalankan ETL dari file Excel tersebut.

Analisa & Kritisi (versi ringkas):

- Saat ini belum ada definisi format output terstandar dan aturan mapping kolom.
- Sumber input hanya disebut “URL Google Drive” tanpa format URL yang diterima (file link vs share link vs folder).
- Belum ada strategi validasi model (model.csv) dan fallback saat model_id tidak ditemukan.
- Belum ada detail modularisasi per model (kontrak fungsi, lokasi file, naming).
- Alur unduh perlu opsi untuk Google Spreadsheet dan .xlsx agar konsisten (export vs download raw).
- Belum ada rencana logging, error handling, atau jejak output (audit trail).

Task (bertahap):

- [x] Definisikan kontrak modul per model (versi awal)
  - [x] Setiap model punya `run_flow`, `get_flow_status`, `download_data`.
  - [x] Lokasi modul mengikuti pola: `models/<data>/<ics_id>/<district>/<model_id>.py`.

- [x] Bangun loader `model.csv`
  - [x] Parser CSV + validasi kolom wajib: `model_id`, `source_gd`, `input_folder`, `input_name`, `output_folder`, `output_name`.
  - [x] Return struktur config yang siap dipakai oleh app.

- [x] Implementasi downloader
  - [x] Deteksi tipe URL: Spreadsheet vs .xlsx.
  - [x] Untuk Spreadsheet: export ke .xlsx.
  - [x] Untuk .xlsx: download langsung.

- [x] Buat routing ke modul per model
  - [x] `app.py` sebagai entry point.
  - [x] Resolve `model_id` -> module -> jalankan pipeline.

- [x] Jalankan ETL per model
  - [x] Implement minimal 1 contoh modul ETL berbasis `model_id`.
  - [x] Output file di `output_folder/output_name.xlsx`.

- [x] UI: Select Model
  - [x] Layout satu baris per model (satu row satu model).
  - [x] Tombol aksi + kartu metadata ringkas per model.

- [x] Update format output training
  - [x] Kolom dasar: `ID Petani | Nama | NIK | Jenis Kelamin`.
  - [x] Detail training: tanggal, nama hadir, jenis kelamin, pre test, post test, kenaikan.
  - [x] Mendukung format BMP/MK/K3 dan format training generik.

- [x] Tambah model baru
  - [x] `petani-it6787-kampar-karsem-01` (file modul + params).

- [ ] Tambah logging & error handling
  - [ ] Logging ke console + file log per run (opsional).
  - [ ] Error jelas untuk model_id tidak ditemukan, download gagal, atau format tidak sesuai.

- [ ] Dokumentasi cara pakai
  - [ ] Contoh perintah dan contoh input URL.
  - [ ] Penjelasan struktur `model.csv`.
