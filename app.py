import csv
import importlib.util
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st


MODEL_CSV_PATH = os.path.join("models", "models.csv")
CHECKLIST_CSV_PATH = os.path.join("models", "checklist.csv")
INPUT_PREFIX = os.path.join("data", "input") + os.sep
MODEL_PREFIX = os.path.join("models") + os.sep


def load_models(csv_path: str = MODEL_CSV_PATH) -> List[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, str]] = []
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
        return rows


def update_model_last_run(model_id: str, timestamp: str, csv_path: str = MODEL_CSV_PATH) -> None:
    if not os.path.exists(csv_path):
        return
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "last_run" not in df.columns:
        df["last_run"] = ""
    if "status" not in df.columns:
        df["status"] = ""
    df.loc[df["model_id"] == model_id, "last_run"] = timestamp
    df.loc[
        (df["model_id"] == model_id) & (df["status"].astype(str).str.len() == 0),
        "status",
    ] = "underdevelopment"
    df.to_csv(csv_path, index=False)


def resolve_module_path(model_row: Dict[str, str]) -> str:
    input_folder = model_row.get("input_folder", "")
    model_id = model_row.get("model_id", "")

    if input_folder.startswith(INPUT_PREFIX):
        module_dir = MODEL_PREFIX + input_folder[len(INPUT_PREFIX) :]
    else:
        module_dir = MODEL_PREFIX

    return os.path.join(module_dir, f"{model_id}.py")


def load_module_from_path(path: str):
    spec = importlib.util.spec_from_file_location("model_module", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def find_model(model_id: str, rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    for row in rows:
        if row.get("model_id") == model_id:
            return row
    return None


st.set_page_config(page_title="Data Tools v1", page_icon="page", layout="wide")
st.title("Data Tools v1")
st.caption("Pipeline per model dengan download dari Google Drive.")

page = st.sidebar.radio("Menu", ["Status", "select model", "result"])

if not os.path.exists(MODEL_CSV_PATH):
    st.error(f"File tidak ditemukan: {MODEL_CSV_PATH}")
    st.stop()

rows = load_models(MODEL_CSV_PATH)
model_ids = [r.get("model_id", "") for r in rows if r.get("model_id")]

if not model_ids:
    st.warning("models.csv kosong atau model_id tidak ditemukan.")
    st.stop()

if page == "Status":
    st.subheader("Status")
    if not os.path.exists(MODEL_CSV_PATH):
        st.warning(f"File tidak ditemukan: {MODEL_CSV_PATH}")
    else:
        status_df = pd.read_csv(MODEL_CSV_PATH, encoding="utf-8-sig")

        def _output_file_path(row: pd.Series) -> str:
            output_folder = str(row.get("output_folder", "") or "")
            output_name = str(row.get("output_name", "") or "")
            if not output_folder or not output_name:
                return ""
            return os.path.join(output_folder, f"{output_name}.xlsx")

        header_cols = st.columns([3, 2, 2, 3])
        header_cols[0].markdown("**model_id**")
        header_cols[1].markdown("**status**")
        header_cols[2].markdown("**last_run**")
        header_cols[3].markdown("**output_file**")

        for _, row in status_df.iterrows():
            model_id = str(row.get("model_id", "") or "")
            status = str(row.get("status", "") or "")
            last_run = str(row.get("last_run", "") or "")
            out_path = _output_file_path(row)

            cols = st.columns([3, 2, 2, 3])
            cols[0].text(model_id)
            cols[1].text(status)
            cols[2].text(last_run)
            with cols[3]:
                if out_path and os.path.exists(out_path):
                    with open(out_path, "rb") as f:
                        st.download_button(
                            label="Download",
                            data=f,
                            file_name=os.path.basename(out_path),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl-{model_id}",
                        )
                else:
                    st.caption("-")

        st.divider()
        st.subheader("Edit Model")

        model_ids = status_df["model_id"].astype(str).tolist()
        if not model_ids:
            st.info("Belum ada data model.")
        else:
            selected_id = st.selectbox("Pilih model", model_ids, key="status_select_model")
            row = status_df[status_df["model_id"] == selected_id].iloc[0]
            row_dict = {k: ("" if pd.isna(v) else str(v)) for k, v in row.items()}

            with st.form("edit_model_form", clear_on_submit=False):
                edited: Dict[str, str] = {}
                for col in status_df.columns:
                    label = col.replace("_", " ").title()
                    edited[col] = st.text_input(label, value=row_dict.get(col, ""))

                submitted = st.form_submit_button("Save")
                if submitted:
                    for col, val in edited.items():
                        status_df.loc[status_df["model_id"] == selected_id, col] = val
                    status_df.to_csv(MODEL_CSV_PATH, index=False)
                    st.success("Perubahan disimpan.")

elif page == "select model":
    st.subheader("Select Model")
    st.caption("Klik tombol Jalankan untuk menetapkan model aktif.")

    st.markdown(
        """
        <style>
        .model-card {
            padding: 8px 10px;
            border: 1px solid #e7e7e7;
            border-radius: 10px;
            background: #0f1115;
            margin-bottom: 6px;
        }
        .model-title {
            font-weight: 700;
            font-size: 0.9rem;
            color: #f2f2f2;
        }
        .model-meta {
            color: #cfcfcf;
            font-size: 0.78rem;
        }
        .status-chip {
            display: inline-block;
            padding: 1px 6px;
            border-radius: 999px;
            font-size: 0.7rem;
            border: 1px solid #3a3f4b;
            background: #151923;
            color: #ffffff;
            margin-right: 4px;
            margin-top: 2px;
        }
        .stButton > button {
            width: 100%;
            border-radius: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if "selected_model" not in st.session_state:
        st.session_state.selected_model = ""
    if "run_model_id" not in st.session_state:
        st.session_state.run_model_id = ""
    if "run_model_time" not in st.session_state:
        st.session_state.run_model_time = ""
    if "run_model_status" not in st.session_state:
        st.session_state.run_model_status = []

    left, right = st.columns([2, 1])
    with left:
        search = st.text_input("Cari model", placeholder="ketik model_id / district / ics")
    with right:
        st.metric("Total Model", len(model_ids))

    st.divider()
    active_label = st.session_state.selected_model or "-"
    st.write(f"Model aktif: `{active_label}`")
    if st.session_state.get("run_model_time"):
        st.caption(f"Last run: {st.session_state.run_model_time}")

    def _match(row: Dict[str, str], query: str) -> bool:
        if not query:
            return True
        q = query.lower()
        return (
            q in row.get("model_id", "").lower()
            or q in row.get("district", "").lower()
            or q in row.get("ics_id", "").lower()
            or q in row.get("ics", "").lower()
            or q in row.get("data", "").lower()
        )

    filtered_rows = []
    for row in rows:
        model_id = row.get("model_id", "")
        if not model_id:
            continue
        if not _match(row, search):
            continue
        filtered_rows.append(row)

    for i in range(0, len(filtered_rows), 2):
        row_cols = st.columns(2)
        chunk = filtered_rows[i : i + 2]
        for col, row in zip(row_cols, chunk):
            model_id = row.get("model_id", "")
            with col:
                btn_cols = st.columns([1, 4])
                with btn_cols[0]:
                    if st.button("Jalankan", key=f"pick-{model_id}"):
                        st.session_state.selected_model = model_id
                        st.session_state.run_model_id = model_id
                        st.session_state.run_model_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        update_model_last_run(model_id, st.session_state.run_model_time)
                        row["last_run"] = st.session_state.run_model_time
                        if not row.get("status"):
                            row["status"] = "underdevelopment"
                        module_path = resolve_module_path(row)
                        if os.path.exists(module_path):
                            module = load_module_from_path(module_path)
                            if hasattr(module, "run_flow"):
                                st.session_state.run_model_status = module.run_flow(model_id=model_id)
                            else:
                                st.session_state.run_model_status = []
                with btn_cols[1]:
                    st.markdown(
                        f"""
                        <div class="model-card">
                            <div class="model-title">{model_id}</div>
                            <div class="model-meta">
                                <span class="status-chip">status: {row.get("status", "")}</span>
                                <span class="status-chip">last run: {row.get("last_run", "")}</span>
                                <span class="status-chip">data: {row.get("data", "")}</span>
                                <span class="status-chip">district: {row.get("district", "")}</span>
                                <span class="status-chip">ics: {row.get("ics_id", "")}</span>
                                <span class="status-chip">ics name: {row.get("ics_name", "")}</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    st.divider()
    model_row = find_model(st.session_state.selected_model, rows) if st.session_state.selected_model else None
    if model_row:
        with st.expander("Detail Model Aktif", expanded=False):
            st.json(model_row)

    st.divider()
    header_cols = st.columns([5, 2])
    header_cols[0].subheader("Info Proses (Terakhir Dijalankan)")
    with header_cols[1]:
        if st.session_state.get("run_model_id"):
            run_row = find_model(st.session_state.run_model_id, rows)
            if run_row:
                output_folder = run_row.get("output_folder", "")
                output_name = run_row.get("output_name", "")
                if output_folder and output_name:
                    output_path = os.path.join(output_folder, f"{output_name}.xlsx")
                    if os.path.exists(output_path):
                        with open(output_path, "rb") as f:
                            st.download_button(
                                label="Download Output",
                                data=f,
                                file_name=os.path.basename(output_path),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download-latest-output",
                            )
    if not st.session_state.get("run_model_id"):
        st.info("Belum ada model yang dijalankan.")
    else:
        def _compact_message(msg: str) -> str:
            if msg.startswith("Read Source GD "):
                return "Source GD: " + msg.replace("Read Source GD ", "")
            if msg.startswith("File sudah ada di input folder"):
                return "File lokal: sudah ada"
            if msg.startswith("sheet list "):
                return "Sheet list: " + msg.replace("sheet list ", "")
            if msg.startswith("Nama Desa : "):
                return "Nama Desa: " + msg.replace("Nama Desa : ", "")
            return msg

        if st.session_state.run_model_status:
            lines = []
            for status, message in st.session_state.run_model_status:
                lines.append(f"[{status}] {_compact_message(message)}")
            st.code("\n".join(lines))
        else:
            run_model_id = st.session_state.run_model_id
            run_row = find_model(run_model_id, rows)
            if not run_row:
                st.warning("Model terakhir dijalankan tidak ditemukan.")
            else:
                module_path = resolve_module_path(run_row)
                if not os.path.exists(module_path):
                    st.warning("Module untuk model terakhir dijalankan belum ada.")
                else:
                    module = load_module_from_path(module_path)
                    if hasattr(module, "get_flow_status"):
                        lines = []
                        for status, message in module.get_flow_status(model_id=run_model_id):
                            lines.append(f"[{status}] {_compact_message(message)}")
                        st.code("\n".join(lines))
                    else:
                        st.info("Checklist belum tersedia di module.")

elif page == "result":
    st.subheader("Result")
    st.caption("Halaman hasil akan diisi setelah proses ETL tersedia.")

    active_model = st.session_state.get("selected_model", "")
    run_model_id = st.session_state.get("run_model_id", "")
    if not active_model:
        st.info("Belum ada model aktif. Silakan tekan tombol Jalankan di menu 'select model'.")
        st.stop()

    model_row = find_model(active_model, rows)
    if not model_row:
        st.warning("Model aktif tidak ditemukan.")
        st.stop()

    st.write(f"Model aktif: `{active_model}`")
    if st.session_state.get("run_model_time"):
        st.caption(f"Last run: {st.session_state.run_model_time}")

    if not run_model_id:
        st.info("Belum ada model yang dijalankan. Silakan tekan tombol Jalankan di menu 'select model'.")
        st.stop()

    module_path = resolve_module_path(model_row)
    st.text(f"Module path: {module_path}")

    if not os.path.exists(module_path):
        st.warning("Module untuk model ini belum ada.")
    else:
        module = load_module_from_path(module_path)

        st.divider()
        st.subheader("Checklist Proses")
        if hasattr(module, "get_flow_status"):
            for status, message in module.get_flow_status(model_id=run_model_id):
                st.write(f"- {status}. {message}")
        else:
            st.info("Checklist belum tersedia di module.")

        st.divider()
        if st.button("Download Data"):
            with st.spinner("Downloading..."):
                if not hasattr(module, "download_data"):
                    st.error("Fungsi download_data() tidak ditemukan di module.")
                else:
                    output_path = module.download_data(model_id=active_model)
                    st.success(f"Download selesai: {output_path}")
