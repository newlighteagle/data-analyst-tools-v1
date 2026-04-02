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

- [ ] Definisikan kontrak modul per model
  - [ ] Setiap model minimal punya fungsi `extract`, `transform`, `load` atau satu `run(model_config, input_path, output_path)`.
  - [ ] Lokasi modul: `models/<model_id>.py` atau `models/<data>/<ics_id>/<district>.py` (pilih satu pola).

- [ ] Bangun loader `model.csv`
  - [ ] Parser CSV + validasi kolom wajib: `model_id`, `source_gd`, `input_folder`, `input_name`, `output_folder`, `output_name`.
  - [ ] Return struktur config yang siap dipakai oleh app.

- [ ] Implementasi downloader
  - [ ] Deteksi tipe URL: Spreadsheet vs .xlsx.
  - [ ] Untuk Spreadsheet: export ke .xlsx.
  - [ ] Untuk .xlsx: download langsung.

- [ ] Buat routing ke modul per model
  - [ ] `app.py` sebagai entry point.
  - [ ] Resolve `model_id` -> module -> jalankan pipeline.

- [ ] Jalankan ETL per model
  - [ ] Implement minimal 1 contoh modul ETL berbasis `model_id`.
  - [ ] Output file di `output_folder/output_name.xlsx`.

- [ ] Tambah logging & error handling
  - [ ] Logging ke console + file log per run (opsional).
  - [ ] Error jelas untuk model_id tidak ditemukan, download gagal, atau format tidak sesuai.

- [ ] Dokumentasi cara pakai
  - [ ] Contoh perintah dan contoh input URL.
  - [ ] Penjelasan struktur `model.csv`.
